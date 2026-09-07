package main

import (
	"fmt"
	"github.com/charmbracelet/x/ansi"
	"math"
	"strings"
	"time"
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
		return "Preparing downloads…\nReading manifests and checking saved files.\n\nPartial downloads will resume automatically."
	}
	var bytes, total int64
	completed := 0
	estimated := false
	for _, id := range m.state.SelectedModelIDs {
		var item *downloadItem
		for i := range p.Models {
			if p.Models[i].ID == id {
				item = &p.Models[i]
				break
			}
		}
		var expected int64
		for _, row := range m.state.Models {
			if row.ID == id {
				expected = int64(row.SizeGB * 1e9)
				break
			}
		}
		if item != nil && item.Total != nil {
			expected = *item.Total
		} else {
			estimated = true
		}
		total += expected
		if item != nil {
			if item.Stage == "completed" {
				completed++
				bytes += expected
			} else {
				bytes += item.Bytes
			}
		}
	}
	// Older disposable fixtures can send only one-file progress.
	if len(p.Models) == 0 {
		bytes = p.Bytes
		total = 0
		if p.Total != nil {
			total = *p.Total
		}
		estimated = false
	}
	prefix := ""
	if estimated {
		prefix = "~"
	}
	clock := &m.downloadClock
	rate := sampleRate(clock.samples, clock.now)
	eta := "ETA calculating…"
	if p.Stage == "verifying" || p.Stage == "publishing" {
		eta = "Verification time varies"
	} else if total > bytes && rate > 0 {
		eta = "ETA ~" + durationLabel(float64(total-bytes)/rate)
	}
	if !clock.lastChange.IsZero() && clock.now.Sub(clock.lastChange) > 15*time.Second && p.Stage == "transferring" {
		eta = "Waiting for download data…"
		rate = 0
	}
	stats := byteSize(bytes) + " / " + prefix + byteSize(total)
	if total == 0 {
		stats = byteSize(bytes) + " · total pending"
	}
	lines := []string{bold.Render(fmt.Sprintf("Overall transfer · %d/%d models ready", completed, len(m.state.SelectedModelIDs))),
		transferBar(bytes, total, width), stats}
	if height >= 8 {
		speed := "Measuring speed…"
		if rate > 0 {
			speed = byteSize(int64(rate)) + "/s"
		}
		if p.Stage == "verifying" || p.Stage == "publishing" {
			speed = "Transfer paused"
		}
		lines = append(lines, speed+" · "+eta)
	} else {
		lines = append(lines, eta)
	}
	name := clean(p.ModelID)
	for _, row := range m.state.Models {
		if row.ID == p.ModelID {
			name = clean(row.Name)
		}
	}
	if height >= 10 {
		elapsed := clock.now.Sub(clock.started).Seconds()
		lines = append(lines, "Elapsed "+durationLabel(elapsed)+" · Saved bytes included; verification follows transfer", "")
	}
	lines = append(lines, bold.Render(name)+" · "+stageLabel(p.Stage))
	if height >= 12 && p.Total != nil {
		lines = append(lines, transferBar(p.Bytes, *p.Total, width))
	}
	remaining := height - len(lines)
	if remaining >= 3 && len(p.Files) > 0 {
		count := min(len(p.Files), (remaining-1)/2)
		first := min(m.bodyScroll, len(p.Files)-1)
		count = min(count, len(p.Files)-first)
		for _, file := range p.Files[first : first+count] {
			label := stageLabel(file.Stage)
			fileRate := sampleRate(clock.files[p.ModelID+"/"+file.ID], clock.now)
			if file.Stage == "transferring" && file.Total != nil && *file.Total > file.Bytes && fileRate > 0 {
				label = "~" + durationLabel(float64(*file.Total-file.Bytes)/fileRate) + " left"
			}
			lines = append(lines, ansi.Truncate(clean(file.ID), max(8, width-len(label)-3), "…")+" · "+label)
			var fileTotal int64
			if file.Total != nil {
				fileTotal = *file.Total
			}
			lines = append(lines, transferBar(file.Bytes, fileTotal, width))
		}
		hint := fmt.Sprintf("Files %d–%d of %d · ↑↓ browse", first+1, first+count, len(p.Files))
		if p.FileCount > len(p.Files) {
			hint = fmt.Sprintf("First %d of %d files shown · ↑↓ browse", len(p.Files), p.FileCount)
		}
		lines = append(lines, hint)
	}
	// Never wrap dashboard rows: the overall bar and cancellation remain pinned.
	for i := range lines {
		lines[i] = ansi.Truncate(lines[i], width, "…")
	}
	return strings.Join(lines[:min(len(lines), height)], "\n")
}
