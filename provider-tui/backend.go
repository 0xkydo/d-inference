package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type backendMessage struct {
	event event
	err   error
}
type backend struct {
	cmd      *exec.Cmd
	input    *os.File
	output   io.ReadCloser
	messages chan backendMessage
	cancel   context.CancelFunc
	writeMu  sync.Mutex
}

func startBackend(path string, args []string) (*backend, error) {
	// No shell, PATH lookup, public listener, or inherited terminal for the child.
	cmd := exec.Command(path, append([]string{"onboarding-session"}, args...)...)
	input, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = input
	cmd.Stderr = io.Discard
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		writer.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		input.Close()
		writer.Close()
		output.Close()
		return nil, err
	}
	input.Close()
	ctx, cancel := context.WithCancel(context.Background())
	b := &backend{cmd: cmd, input: writer, output: output, messages: make(chan backendMessage, 16), cancel: cancel}
	go b.read(ctx)
	return b, nil
}
func (b *backend) read(ctx context.Context) {
	defer close(b.messages)
	scanner := bufio.NewScanner(b.output)
	scanner.Buffer(make([]byte, 4096), maxEventBytes)
	for scanner.Scan() {
		e, err := decodeEvent(scanner.Bytes())
		select {
		case b.messages <- backendMessage{e, err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
	err := errors.New("onboarding session closed; reopen with darkbloom start --tui to resume")
	if scanner.Err() != nil {
		err = errors.New("onboarding session output was interrupted")
	}
	select {
	case b.messages <- backendMessage{err: err}:
	case <-ctx.Done():
	}
}
func (b *backend) send(c command) error {
	data, err := json.Marshal(c)
	if err != nil || len(data) > maxCommandBytes {
		return errors.New("invalid onboarding command")
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if err = b.input.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = b.input.Write(append(data, '\n'))
	return err
}
func (b *backend) close() error {
	// Command EOF is the cancellation protocol, including on terminal HUP.
	// Swift cancels and awaits its operation before releasing the setup/cache locks.
	b.input.Close()
	b.cancel()
	b.output.Close()
	done := make(chan error, 1)
	go func() { done <- b.cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		_ = b.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-done:
		return nil
	case <-time.After(3 * time.Second):
		// Only this foreground child, never launchd or an already started provider.
		_ = b.cmd.Process.Kill()
		<-done
		return errors.New("the onboarding session did not stop promptly; partial downloads were retained")
	}
}
