package main

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/charmbracelet/lipgloss"
)

var muted = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
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
	if m.width < 32 || m.height < 14 {
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
			body = bold.Render("Finish the approval in System Settings.") + "\n\nOpen General → Device Management, select the Darkbloom profile and click Install or Enroll. Follow the macOS prompts.\n\n" +
				"You can leave this terminal open. We'll detect the installed profile and take you to account linkage automatically.\n\nIf the profile is missing, press o to reopen it."
			footer = "Checking enrollment automatically… · Enter check now · o reopen profile · q quit"
		case "account":
			title = "2. Link your account"
			body = success.Render("✓ Device enrollment confirmed locally.") + "\n\n" + bold.Render("Next: connect your Darkbloom account.") + "\n\nLink this Mac to receive earnings for serving inference.\nYour browser will open so you can approve this Mac."
			footer = "Press Enter to open your browser · q finish later"
		case "models":
			title = "3. Choose models to download"
			body, focusLine = m.modelList()
			footer = "Enter download selection · ↑↓ move · Space select · h additional · q quit"
		case "downloading":
			title = "4. Download and verify"
			body = ""
			footer = "q cancel and keep files · d file details · ↑↓ browse models"
			if m.downloadDetails {
				footer = "q cancel and keep files · d back to models · ↑↓ browse files"
			}
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
	if m.busy && (m.state == nil || (m.state.Phase != "downloading" && m.state.Phase != "enrollment_pending")) {
		footer = "Working… · q / Ctrl-C cancel"
	}
	return m.renderPage(title, body, footer, focusLine)
}
