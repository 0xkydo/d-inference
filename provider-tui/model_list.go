package main

import (
	"fmt"
	"strings"
)

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
