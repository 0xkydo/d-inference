package main

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

var bold = lipgloss.NewStyle().Bold(true)
var success = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
var failure = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))

// Catalog names, profile observations, code URLs and filenames are data, never
// terminal escape sequences (including OSC hyperlinks/clipboard commands).
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}
func (m *model) View() string {
	if m.quitting {
		return ""
	}
	if m.width < 24 || m.height < 10 {
		return "Enlarge the terminal.\nq / Ctrl-C to quit"
	}
	title := "Darkbloom"
	var body, footer string
	focusLine := -1
	if m.state == nil {
		body = "Checking this Mac…"
		footer = "r retry · q quit"
	} else {
		s := m.state
		switch s.Phase {
		case "enrollment":
			title = "1. Enroll this Mac"
			body = "Device verification helps protect private requests by checking the identity and security settings of Macs serving them.\n\n" +
				bold.Render("The Darkbloom profile is read-only.") + "\nIt can read device information, installed profiles, and security settings. It cannot read personal files, change settings, install apps, or lock, erase, or remotely control your Mac.\n\n" +
				"You approve installation in System Settings → General → Device Management. You can remove the profile there."
			footer = "Enter open device enrollment · q quit"
		case "enrollment_pending":
			title = "1. Complete enrollment in macOS"
			body = "In System Settings → General → Device Management, select the Darkbloom profile and click Install or Enroll. Follow the macOS prompts.\n\nAfter clicking OK, return here. Darkbloom checks the actual macOS enrollment before continuing."
			footer = "Enter check enrollment · q quit"
		case "account":
			title = "2. Link your account"
			body = success.Render("Device enrollment confirmed locally.") + "\n\nLink this Mac to your Darkbloom account to receive earnings for serving inference. Your browser will open for approval."
			footer = "Enter open account linkage · q quit"
		case "models":
			title = "3. Choose models to download"
			body, focusLine = m.modelList()
			footer = "↑↓ move · Space select · h additional · Enter confirm · q quit"
		case "downloading":
			title = "4. Download and verify"
			body = "Your selected models are being downloaded and verified.\n\nClosing this terminal cancels downloads. Completed and partial files are kept for resume."
			footer = "q / Ctrl-C cancel and keep files"
		case "ready":
			title = "5. Ready to start Darkbloom"
			body = success.Render("Enrollment, account linkage, and selected downloads are complete.") + "\n\n" +
				"Darkbloom will run in the background and start when you log in. It continues after this terminal closes. Use darkbloom stop to stop it.\n\n" +
				"Only selected models that fit will be enabled. Starting accepts the Terms of Service: https://darkbloom.dev/terms.html\n\n" + bold.Render("Press Enter only when you want to start the provider.")
			footer = "Enter start Darkbloom · q finish later"
		case "started":
			title = "Start requested"
			body = success.Render("The background service was launched.") + "\n\nClosing this terminal leaves the provider running.\n\nUse darkbloom status and darkbloom doctor to check model readiness and coordinator verification. A launch is not proof of network trust.\n\nUse darkbloom stop to stop serving."
			footer = "Enter / q close onboarding"
		}
		if s.ProviderRunning && s.Phase == "ready" {
			body += "\n\nEnter applies this selection and restarts the running provider."
		}
		if s.ProviderRunning && s.Phase != "started" {
			body += "\n\nA provider process is already running; closing setup leaves it running."
		}
		if s.Notice != nil {
			body += "\n\n" + failure.Render("Crash-recovery watchdog could not be installed. Check darkbloom doctor.")
		}
	}
	if m.code != nil {
		body += "\n\n" + bold.Render("Approve this Mac in your browser:") + "\n" + clean(m.code.URL) +
			"\nCode: " + bold.Render(clean(m.code.Code)) + fmt.Sprintf("\nExpires in %d minutes. Return here after approval.", m.code.ExpiresIn/60)
	}
	if p := m.progress; p != nil {
		body += "\n\n" + clean(p.ModelID) + "\n" + clean(p.File) + "\n"
		switch p.Stage {
		case "verifying":
			body += "Verifying integrity…"
		case "publishing":
			body += "Publishing verified files…"
		case "completed":
			body += success.Render("Download verified and published.")
		default:
			body += fmt.Sprintf("%.1f MB transferred", float64(p.Bytes)/1e6)
			if p.Total != nil && *p.Total > 0 {
				body += fmt.Sprintf(" / %.1f MB (%d%%)", float64(*p.Total)/1e6, min(100, max(0, int(float64(p.Bytes)/float64(*p.Total)*100))))
			}
			body += " · verification follows transfer"
		}
	}
	if m.busy && (m.state == nil || m.state.Phase != "downloading") {
		footer = "Working… · q / Ctrl-C cancel"
	}
	if m.problem != "" {
		footer = failure.Render(clean(m.problem)) + "\n" + footer
	}
	width := m.width - 4
	header := bold.Render(title) + "\n\n"
	footer = ansi.Wrap(footer, width, "")
	lines := strings.Split(ansi.Wrap(body, width, ""), "\n")
	available := max(1, m.height-5-strings.Count(footer, "\n"))
	if len(lines) > available {
		first := 0
		if focusLine >= 0 {
			// Find the actual focused rendered row after wrapping for this terminal.
			for i, line := range lines {
				if strings.HasPrefix(line, "›") {
					focusLine = i
					break
				}
			}
			first = min(max(0, focusLine-available/2), len(lines)-available)
		} else if m.code != nil || m.progress != nil {
			first = len(lines) - available
		}
		lines = lines[first : first+available]
	}
	return header + strings.Join(lines, "\n") + "\n\n" + footer
}
func (m *model) modelList() (string, int) {
	lines := []string{fmt.Sprintf("%.0f GiB total RAM · %.1f GiB model budget", m.state.MemoryGiB, m.state.BudgetGiB),
		"Fit estimates each model separately; running apps still matter when loading.", ""}
	group := ""
	focused := -1
	for i, row := range m.visible() {
		next := "Available to download"
		if row.Downloaded {
			next = "Downloaded"
		} else if row.FitReason != nil {
			next = "Additional models"
		}
		if next != group {
			lines = append(lines, bold.Render(next))
			group = next
			if next == "Additional models" {
				lines = append(lines, "These may not fit or require different hardware. Downloading does not enable serving.")
			}
		}
		cursor, mark := " ", " "
		if i == m.cursor {
			cursor = "›"
			focused = len(lines)
		}
		if m.selected[row.ID] {
			mark = "x"
		}
		line := fmt.Sprintf("%s [%s] %s · %.1f GB", cursor, mark, clean(row.Name), row.SizeGB)
		if row.Resumable {
			line += " · resume"
		}
		lines = append(lines, line)
		if row.FitReason != nil {
			lines = append(lines, "    "+clean(*row.FitReason))
		}
	}
	hidden, count := 0, 0
	size := 0.0
	for _, row := range m.state.Models {
		if !row.Downloaded && row.FitReason != nil {
			hidden++
		}
		if m.selected[row.ID] {
			count++
			if !row.Downloaded {
				size += row.SizeGB
			}
		}
	}
	if hidden > 0 {
		lines = append(lines, fmt.Sprintf("h show/hide %d additional models", hidden))
	}
	lines = append(lines, fmt.Sprintf("%d selected · %.1f GB to download", count, size))
	return strings.Join(lines, "\n"), focused
}
