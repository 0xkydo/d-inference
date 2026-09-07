package main

import (
	"fmt"
	"github.com/charmbracelet/x/ansi"
	"math"
	"strings"
)

func byteSize(n int64) string {
	if n >= 1e9 {
		return fmt.Sprintf("%.2f GB", float64(n)/1e9)
	}
	if n >= 1e6 {
		return fmt.Sprintf("%.1f MB", float64(n)/1e6)
	}
	return fmt.Sprintf("%.0f KB", float64(n)/1e3)
}
func durationLabel(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", int(math.Ceil(max(0, seconds))))
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm %02ds", int(seconds)/60, int(seconds)%60)
	}
	return fmt.Sprintf("%dh %02dm", int(seconds)/3600, int(seconds)%3600/60)
}
func transferBar(bytes, total int64, width int) string {
	width = max(3, width-7)
	if total <= 0 {
		return muted.Render(strings.Repeat("─", width)) + "   —"
	}
	fraction := min(1.0, max(0.0, float64(bytes)/float64(total)))
	filled := int(fraction * float64(width))
	return success.Render(strings.Repeat("━", filled)) + muted.Render(strings.Repeat("─", width-filled)) + fmt.Sprintf(" %3d%%", int(fraction*100))
}
func stageLabel(stage string) string {
	switch stage {
	case "queued":
		return "Waiting"
	case "verifying":
		return "Checking file integrity…"
	case "publishing":
		return "Saving verified model…"
	case "completed":
		return "Verified"
	default:
		return "Downloading"
	}
}
func (m *model) downloadView(width, height int) string {
	p := m.progress
	if p == nil {
		return "Preparing downloads…\n\nGrab a drink. Keep this terminal open and your Mac awake."
	}
	rows, bytes, total, completed := m.downloadModels()
	rate := sampleRate(m.downloadClock.samples, m.downloadClock.now)
	eta := "Download ETA · calculating…"
	if p.Stage == "verifying" || p.Stage == "publishing" {
		eta = "Checking your downloads…"
	} else if total > bytes && rate > 0 {
		eta = "Download ETA · ~" + durationLabel(float64(total-bytes)/rate) + " remaining"
	}
	if m.downloadStalled() {
		eta = "Waiting for download data…"
	}
	lines := []string{bold.Render(eta), transferBar(bytes, total, width),
		fmt.Sprintf("%d of %d models ready", completed, len(rows))}
	if height >= 10 {
		lines = append(lines, "", ansi.Truncate("Grab a drink. Keep this terminal open", width, "…"), "and your Mac awake and connected.", "")
	}
	if m.downloadDetails {
		lines = append(lines, m.downloadFileDetails(width, height-len(lines), rate)...)
	} else {
		remaining := height - len(lines)
		if remaining > 0 && len(rows) > 0 {
			perRow := 2
			if remaining < 3 {
				perRow = 1
			}
			count := min(len(rows), max(1, (remaining-1)/perRow))
			first := min(m.bodyScroll, len(rows)-1)
			count = min(count, len(rows)-first)
			for _, row := range rows[first : first+count] {
				label := stageLabel(row.Stage)
				if row.Stage == "completed" {
					label = "✓ Ready"
				}
				if row.Stage == "queued" {
					label = "Queued"
				}
				name := ansi.Truncate(clean(row.ID), max(1, width-ansi.StringWidth(label)-3), "…")
				lines = append(lines, name+" · "+label)
				if perRow == 2 {
					var expected int64
					if row.Total != nil {
						expected = *row.Total
					}
					lines = append(lines, transferBar(row.Bytes, expected, width))
				}
			}
			if count < len(rows) && len(lines) < height {
				lines = append(lines, fmt.Sprintf("Models %d–%d of %d · ↑↓ browse", first+1, first+count, len(rows)))
			}
		}
	}
	for i := range lines {
		lines[i] = ansi.Truncate(lines[i], width, "…")
	}
	return strings.Join(lines[:min(len(lines), height)], "\n")
}
