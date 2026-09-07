package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/term"
)

// Keep the file interface so Bubble Tea can enter and restore raw mode. Bubble
// Tea itself ignores input EOF; explicitly cancel our owning context on loss.
type terminalInput struct {
	*os.File
	cancel context.CancelFunc
}

func (r terminalInput) Read(p []byte) (int, error) {
	n, err := r.File.Read(p)
	if err != nil {
		r.cancel()
	}
	return n, err
}
func run() (result error) {
	backendPath := flag.String("backend", "", "Absolute path to the sibling Swift darkbloom executable")
	config := flag.String("config", "", "Provider config path")
	coordinator := flag.String("coordinator-url", "", "Coordinator URL")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if !term.IsTerminal(os.Stdin.Fd()) || !term.IsTerminal(os.Stdout.Fd()) || os.Getenv("TERM") == "dumb" {
		return errors.New("Bubble Tea requires a terminal; use darkbloom start for the plain CLI")
	}
	if *backendPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		exe, err = filepath.EvalSymlinks(exe)
		if err != nil {
			return err
		}
		*backendPath = filepath.Join(filepath.Dir(exe), "darkbloom")
	}
	if !filepath.IsAbs(*backendPath) {
		return errors.New("backend path must be absolute")
	}
	args := []string{}
	if *config != "" {
		args = append(args, "--config", *config)
	}
	if *coordinator != "" {
		args = append(args, "--coordinator-url", *coordinator)
	}
	b, err := startBackend(*backendPath, args)
	if err != nil {
		return errors.New("could not launch the Swift onboarding session; install the complete bundle")
	}
	defer func() {
		if err := b.close(); result == nil {
			result = err
		}
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)
	defer cancel()
	stopOnSignal := context.AfterFunc(ctx, func() { _ = b.input.Close() })
	defer stopOnSignal()
	model := newModel(b.send, receiveFrom(b))
	model.stop = func() { _ = b.input.Close() }
	p := tea.NewProgram(model, tea.WithContext(ctx), tea.WithInput(terminalInput{os.Stdin, cancel}),
		tea.WithOutput(os.Stdout), tea.WithAltScreen(), tea.WithoutSignalHandler())
	_, err = p.Run()
	if errors.Is(err, tea.ErrProgramKilled) || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
