package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/x/ansi"
)

var setupSteps = []string{"Enroll", "Account", "Models", "Download", "Start"}

func (m *model) currentStep() int {
	if m.state == nil {
		return 0
	}
	switch m.state.Phase {
	case "account":
		return 1
	case "models":
		return 2
	case "downloading":
		return 3
	case "ready", "started":
		return 4
	default:
		return 0
	}
}

func (m *model) completedSteps() [5]bool {
	if m.state == nil {
		return [5]bool{}
	}
	s := m.state
	return [5]bool{s.Enrolled, s.Linked,
		s.Phase == "downloading" || s.Phase == "ready" || s.Phase == "started",
		s.Phase == "ready" || s.Phase == "started", s.Phase == "started"}
}

func (m *model) setupHeader(width int) string {
	current, done := m.currentStep(), m.completedSteps()
	brand := bold.Render("DARKBLOOM") + "  /  SETUP"
	if width < 68 || m.height < 22 {
		return brand + "\n" + fmt.Sprintf("Step %d of 5 · %s", current+1, setupSteps[current])
	}
	var steps []string
	for i, label := range setupSteps {
		item := fmt.Sprintf("%d %s", i+1, label)
		if done[i] {
			item = success.Render("✓ " + label)
		} else if i == current {
			item = bold.Render("● " + item)
		}
		steps = append(steps, item)
	}
	return brand + "\nSet up this Mac. Start serving when you're ready.\n\n" + strings.Join(steps, "  ›  ")
}

// Keep the brand, position and primary action visible while only the body
// scrolls. Wide terminals use a bounded reading width; small ones stay compact.
func (m *model) renderPage(title, body, footer string, focusLine int) string {
	width := min(94, m.width-4)
	rule := strings.Repeat("─", width)
	header := m.setupHeader(width) + "\n" + rule + "\n" + ansi.Wrap(bold.Render(title), width, "") + "\n\n"
	parts := strings.SplitN(footer, " · ", 2)
	footer = bold.Render(parts[0])
	if len(parts) == 2 {
		controls := strings.Split(parts[1], " · ")
		for i, control := range controls {
			if strings.HasPrefix(control, "q ") {
				controls = append([]string{control}, append(controls[:i], controls[i+1:]...)...)
				break
			}
		}
		footer += "\n" + strings.Join(controls, " · ")
	}
	footer = ansi.Wrap(footer, width, "")
	// Errors remain visible without pushing the action or header off-screen.
	if m.problem != "" {
		footer = failure.Render(ansi.Truncate(clean(m.problem), width, "…")) + "\n" + footer
	}
	footer = rule + "\n" + footer
	available := max(1, m.height-1-strings.Count(header, "\n")-strings.Count(footer, "\n")-2)
	if m.state != nil && m.state.Phase == "downloading" {
		body = m.downloadView(width, available)
	}
	lines := strings.Split(ansi.Wrap(body, width, ""), "\n")
	if len(lines) > available {
		showHint := focusLine < 0 && m.code == nil && available > 1
		visible := available
		if showHint {
			visible--
		}
		first := min(m.bodyScroll, len(lines)-visible)
		if focusLine >= 0 {
			for i, line := range lines {
				if strings.HasPrefix(line, "›") {
					focusLine = i
					break
				}
			}
			first = min(max(0, focusLine-visible/2), len(lines)-visible)
		} else if m.code != nil {
			first = len(lines) - visible
		}
		lines = lines[first : first+visible]
		// The header/footer stay in place when reading longer enrollment text.
		if showHint {
			lines = append(append([]string{}, lines...), "PgUp / PgDn to read more")
		}
	}
	for len(lines) < available {
		lines = append(lines, "")
	}
	page := header + strings.Join(lines, "\n") + "\n\n" + footer
	return "  " + strings.ReplaceAll(page, "\n", "\n  ")
}
