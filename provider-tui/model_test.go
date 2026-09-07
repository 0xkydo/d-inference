package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
)

func fixtures(t *testing.T) []event {
	t.Helper()
	data, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Commands []command         `json:"commands"`
		Events   []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Commands {
		data, _ := json.Marshal(c)
		var got command
		if err := json.Unmarshal(data, &got); err != nil || got.Action != c.Action {
			t.Fatal("command round trip")
		}
	}
	var out []event
	for _, data := range f.Events {
		e, err := decodeEvent(data)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}
func TestSharedContractAndBounds(t *testing.T) {
	fixtures(t)
	for _, data := range []string{`{}`, `{"version":2,"kind":"error","error":"busy"}`, `{"version":1,"kind":"unknown"}`, strings.Repeat("x", maxEventBytes+1)} {
		if _, err := decodeEvent([]byte(data)); err == nil {
			t.Fatalf("accepted invalid frame: %.50s", data)
		}
	}
}
func TestActualModelRequiresExplicitActions(t *testing.T) {
	var commands []command
	m := newModel(func(c command) error { commands = append(commands, c); return nil }, func() backendMessage { return backendMessage{} })
	e := fixtures(t)[1]
	m.Update(backendMessage{event: e})
	if len(commands) != 0 {
		t.Fatal("snapshot caused action")
	}
	if strings.Contains(m.View(), "Downloaded") {
		t.Fatal("empty downloaded group")
	}
	if strings.Contains(m.View(), "Large model") {
		t.Fatal("additional model not hidden")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("h")})
	if !strings.Contains(m.View(), "Large model") {
		t.Fatal("additional model did not expand")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeySpace})
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	cmd()
	if len(commands) != 1 || commands[0].Action != "select_models" || len(commands[0].ModelIDs) != 2 {
		t.Fatalf("selection: %+v", commands)
	}
	ready := *m.state
	ready.Revision++
	ready.Phase = "ready"
	m.Update(backendMessage{event: event{Kind: "snapshot", Snapshot: &ready}})
	if len(commands) != 1 {
		t.Fatal("download completion started provider")
	}
	_, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	cmd()
	if commands[1].Action != "start" {
		t.Fatal("final Enter did not start")
	}
}
func TestResizeSanitizationAndCancellation(t *testing.T) {
	m := newModel(func(command) error { t.Fatal("quit sent action"); return nil }, func() backendMessage { return backendMessage{} })
	m.Update(backendMessage{event: fixtures(t)[1]})
	m.state.Models[0].Name = "model\x1b]52;clipboard\a\nforged\u202e"
	for _, size := range []tea.WindowSizeMsg{{Width: 80, Height: 24}, {Width: 32, Height: 12}, {Width: 12, Height: 6}} {
		m.Update(size)
		view := m.View()
		if strings.Contains(view, "\x1b]52") || strings.Contains(view, "\u202e") {
			t.Fatal("untrusted escape rendered")
		}
		if size.Width >= 24 {
			for _, line := range strings.Split(view, "\n") {
				if ansi.StringWidth(line) > size.Width {
					t.Fatalf("overflow: %q", line)
				}
			}
		}
	}
	m.busy = true
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatal("Ctrl-C did not quit during operation")
	}
}
