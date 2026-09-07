package main

import (
	tea "github.com/charmbracelet/bubbletea"
	"time"
)

type downloadTick struct{}

func nextDownloadTick() tea.Cmd {
	return tea.Tick(time.Second, func(time.Time) tea.Msg { return downloadTick{} })
}

type transferSample struct {
	at    time.Time
	bytes int64
}
type downloadClock struct {
	modelID                  string
	started, now, lastChange time.Time
	samples                  []transferSample
	files                    map[string][]transferSample
}

func appendSample(samples []transferSample, bytes int64, now time.Time) []transferSample {
	if len(samples) > 0 && bytes < samples[len(samples)-1].bytes {
		samples = nil
	}
	samples = append(samples, transferSample{now, bytes})
	for len(samples) > 2 && now.Sub(samples[1].at) > 10*time.Second {
		samples = samples[1:]
	}
	return samples
}
func (c *downloadClock) observe(p *progress, now time.Time) {
	c.now = now
	if c.started.IsZero() {
		c.started = now
	}
	if p.NetworkBytes != nil {
		if len(c.samples) == 0 || *p.NetworkBytes != c.samples[len(c.samples)-1].bytes {
			c.lastChange = now
		}
		c.samples = appendSample(c.samples, *p.NetworkBytes, now)
	}
	if c.files == nil || c.modelID != p.ModelID {
		c.modelID = p.ModelID
		c.files = map[string][]transferSample{}
	}
	for _, f := range p.Files {
		key := p.ModelID + "/" + f.ID
		c.files[key] = appendSample(c.files[key], f.Bytes, now)
	}
}
func sampleRate(samples []transferSample, now time.Time) float64 {
	if len(samples) < 2 {
		return 0
	}
	first, last := samples[0], samples[len(samples)-1]
	seconds := now.Sub(first.at).Seconds()
	if seconds < 1 || now.Sub(last.at) > 15*time.Second {
		return 0
	}
	return float64(max(int64(0), last.bytes-first.bytes)) / seconds
}
