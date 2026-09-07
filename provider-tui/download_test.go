package main

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	"strings"
	"testing"
	"time"
)

func int64ptr(n int64) *int64 { return &n }
func downloadModel() *model {
	m := newModel(func(command) error { return nil }, func() backendMessage { return backendMessage{} })
	m.width, m.height = 88, 28
	m.state = &snapshot{Phase: "downloading", Revision: 1, Enrolled: true, Linked: true, SelectedModelIDs: []string{"one", "two"}, Models: []catalogModel{{ID: "one", Name: "First model", SizeGB: 4}, {ID: "two", Name: "Next model", SizeGB: 2}}}
	m.busy = true
	m.progress = &progress{ModelID: "one", Bytes: 2e9, Total: int64ptr(4e9), Stage: "transferring", Models: []downloadItem{{ID: "one", Bytes: 2e9, Total: int64ptr(4e9), Stage: "transferring"}, {ID: "two", Stage: "queued"}}, Files: []downloadItem{{ID: "weights-1", Bytes: 1e9, Total: int64ptr(2e9), Stage: "transferring"}, {ID: "weights-2", Bytes: 1e9, Total: int64ptr(2e9), Stage: "transferring"}, {ID: "config.json", Bytes: 32, Total: int64ptr(32), Stage: "completed"}}, FileCount: 3}
	return m
}
func TestDownloadDashboardBoundsAndScroll(t *testing.T) {
	m := downloadModel()
	for _, size := range [][2]int{{88, 28}, {42, 16}, {32, 14}, {120, 40}} {
		m.width, m.height = size[0], size[1]
		view := m.View()
		if !strings.Contains(view, "Overall transfer") || !strings.Contains(view, "q cancel") {
			t.Fatalf("missing pinned controls at %v: %s", size, view)
		}
		lines := strings.Split(view, "\n")
		if len(lines) > m.height {
			t.Fatalf("height overflow at %v: %d", size, len(lines))
		}
		for _, line := range lines {
			if ansi.StringWidth(line) > m.width {
				t.Fatalf("width overflow at %v", size)
			}
		}
	}
	m.width, m.height = 88, 28
	if !strings.Contains(m.View(), "weights-1") {
		t.Fatal("first file hidden")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	if !strings.Contains(m.View(), "config.json") {
		t.Fatal("cannot browse files while busy")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if !m.busy || m.nextID != 0 {
		t.Fatal("download Enter advanced workflow")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	if !m.quitting {
		t.Fatal("download cancellation blocked")
	}
}
func TestDownloadRateExcludesSavedBytesAndExpires(t *testing.T) {
	now := time.Unix(100, 0)
	c := downloadClock{}
	p := &progress{ModelID: "one", NetworkBytes: int64ptr(0), Files: []downloadItem{{ID: "a", Bytes: 9e9}}}
	c.observe(p, now)
	p.NetworkBytes = int64ptr(20e6)
	p.Files[0].Bytes += 20e6
	c.observe(p, now.Add(2*time.Second))
	if got := sampleRate(c.samples, c.now); got != 10e6 {
		t.Fatalf("resumed bytes inflated rate: %f", got)
	}
	if got := sampleRate(c.files["one/a"], c.now); got != 10e6 {
		t.Fatalf("file rate: %f", got)
	}
	if sampleRate(c.samples, now.Add(30*time.Second)) != 0 {
		t.Fatal("stale rate retained")
	}
	m := downloadModel()
	m.downloadClock = c
	if !strings.Contains(m.View(), "ETA ~") {
		t.Fatal("missing ETA")
	}
	m.progress.Stage = "verifying"
	if !strings.Contains(m.View(), "Verification time varies") || strings.Contains(m.View(), "ETA ~") {
		t.Fatal("transfer ETA promises verification completion")
	}
	m.progress.Files[0].ID = "bad\x1b]52;clipboard\x07"
	if strings.Contains(m.View(), "\x1b]52") {
		t.Fatal("filename terminal injection")
	}
}
