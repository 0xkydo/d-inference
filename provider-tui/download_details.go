package main

import (
	"fmt"
	"github.com/charmbracelet/x/ansi"
	"time"
)

// Resolve every selected model, including queued models and previously finished
// downloads. Catalog sizes are estimates until a manifest supplies exact bytes.
func (m *model) downloadModels() (rows []downloadItem, bytes, total int64, completed int) {
	for _, id := range m.state.SelectedModelIDs {
		row := downloadItem{ID: id, Stage: "queued"}
		var expected int64
		for _, item := range m.progress.Models {
			if item.ID == id {
				row = item
				break
			}
		}
		if len(m.progress.Models) == 0 && m.progress.ModelID == id {
			row = downloadItem{ID: id, Bytes: m.progress.Bytes, Total: m.progress.Total, Stage: m.progress.Stage}
		}
		for _, catalog := range m.state.Models {
			if catalog.ID == id {
				row.ID = catalog.Name
				expected = int64(catalog.SizeGB * 1e9)
				break
			}
		}
		if row.Total != nil {
			expected = *row.Total
		}
		row.Total = &expected
		if row.Stage == "completed" {
			completed++
			row.Bytes = expected
		}
		bytes += row.Bytes
		total += expected
		rows = append(rows, row)
	}
	return
}
func (m *model) downloadStalled() bool {
	c := &m.downloadClock
	return m.progress.Stage == "transferring" && !c.lastChange.IsZero() && c.now.Sub(c.lastChange) > 15*time.Second
}
func (m *model) downloadFileDetails(width, height int, rate float64) []string {
	if height <= 0 {
		return nil
	}
	p := m.progress
	name := clean(p.ModelID)
	for _, row := range m.state.Models {
		if row.ID == p.ModelID {
			name = clean(row.Name)
		}
	}
	lines := []string{bold.Render("Files · " + name)}
	if height >= 5 {
		stats := "Elapsed " + durationLabel(m.downloadClock.now.Sub(m.downloadClock.started).Seconds())
		if rate > 0 && !m.downloadStalled() && p.Stage == "transferring" {
			stats += " · " + byteSize(int64(rate)) + "/s"
		}
		lines = append(lines, stats)
	}
	if len(p.Files) == 0 {
		return append(lines, "Reading file inventory…")
	}
	count := min(len(p.Files), max(0, (height-len(lines)-1)/2))
	if count == 0 {
		return append(lines, "Enlarge terminal for file details.")
	}
	first := min(m.bodyScroll, len(p.Files)-1)
	count = min(count, len(p.Files)-first)
	for _, file := range p.Files[first : first+count] {
		label := stageLabel(file.Stage)
		rate := sampleRate(m.downloadClock.files[p.ModelID+"/"+file.ID], m.downloadClock.now)
		if file.Stage == "transferring" && file.Total != nil && *file.Total > file.Bytes && rate > 0 {
			label = "~" + durationLabel(float64(*file.Total-file.Bytes)/rate) + " left"
		}
		lines = append(lines, ansi.Truncate(clean(file.ID), max(1, width-ansi.StringWidth(label)-3), "…")+" · "+label)
		var total int64
		if file.Total != nil {
			total = *file.Total
		}
		lines = append(lines, transferBar(file.Bytes, total, width))
	}
	hint := fmt.Sprintf("Files %d–%d of %d · ↑↓ browse", first+1, first+count, len(p.Files))
	if p.FileCount > len(p.Files) {
		hint = fmt.Sprintf("First %d of %d files shown", len(p.Files), p.FileCount)
	}
	return append(lines, hint)
}
