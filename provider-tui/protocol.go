package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const protocolVersion = 1
const maxCommandBytes = 16384
const maxEventBytes = 262144

type command struct {
	Version  int      `json:"version"`
	ID       int      `json:"id"`
	Action   string   `json:"action"`
	Revision *int     `json:"revision,omitempty"`
	ModelIDs []string `json:"modelIDs,omitempty"`
}
type catalogModel struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	SizeGB     float64 `json:"sizeGB"`
	Downloaded bool    `json:"downloaded"`
	Resumable  bool    `json:"resumable"`
	FitReason  *string `json:"fitReason,omitempty"`
}
type snapshot struct {
	Phase            string         `json:"phase"`
	Revision         int            `json:"revision"`
	Enrolled         bool           `json:"enrolled"`
	Linked           bool           `json:"linked"`
	ProviderRunning  bool           `json:"providerRunning"`
	ObservedAt       float64        `json:"observedAt"`
	MemoryGiB        float64        `json:"memoryGiB"`
	BudgetGiB        float64        `json:"budgetGiB"`
	Models           []catalogModel `json:"models"`
	SelectedModelIDs []string       `json:"selectedModelIDs"`
	Notice           *string        `json:"notice,omitempty"`
}
type downloadItem struct {
	ID    string `json:"id"`
	Bytes int64  `json:"bytes"`
	Total *int64 `json:"total,omitempty"`
	Stage string `json:"stage"`
}
type progress struct {
	Models       []downloadItem `json:"models,omitempty"`
	Files        []downloadItem `json:"files,omitempty"`
	FileCount    int            `json:"fileCount,omitempty"`
	NetworkBytes *int64         `json:"networkBytes,omitempty"`
	ModelID      string         `json:"modelID"`
	File         string         `json:"file"`
	Bytes        int64          `json:"bytes"`
	Total        *int64         `json:"total,omitempty"`
	Stage        string         `json:"stage"`
}
type linkCode struct {
	Code      string `json:"code"`
	URL       string `json:"url"`
	ExpiresIn int    `json:"expiresIn"`
}
type event struct {
	Version   int       `json:"version"`
	Kind      string    `json:"kind"`
	CommandID int       `json:"commandID"`
	Snapshot  *snapshot `json:"snapshot,omitempty"`
	Progress  *progress `json:"progress,omitempty"`
	LinkCode  *linkCode `json:"linkCode,omitempty"`
	Error     *string   `json:"error,omitempty"`
}

func decodeEvent(data []byte) (event, error) {
	var e event
	if len(data) > maxEventBytes {
		return e, errors.New("session event is too large")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, errors.New("invalid session event")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return e, errors.New("invalid session framing")
	}
	if e.Version != protocolVersion {
		return e, errors.New("incompatible onboarding versions; install the complete bundle")
	}
	valid := false
	switch e.Kind {
	case "snapshot":
		if e.Snapshot != nil && e.Snapshot.Revision > 0 && len(e.Snapshot.Models) <= 128 {
			switch e.Snapshot.Phase {
			case "enrollment", "enrollment_pending", "account", "models", "downloading", "ready", "started":
				valid = true
			}
		}
	case "progress":
		p := e.Progress
		valid = p != nil && len(p.Models) <= 128 && len(p.Files) <= 128
		if valid {
			valid = validDownloadItem(downloadItem{ID: p.ModelID, Bytes: p.Bytes, Total: p.Total, Stage: p.Stage}) && p.FileCount >= 0 && (p.NetworkBytes == nil || *p.NetworkBytes >= 0)
			for _, item := range append(append([]downloadItem{}, p.Models...), p.Files...) {
				valid = valid && validDownloadItem(item)
			}
		}
	case "link_code":
		valid = e.LinkCode != nil
	case "error":
		valid = e.Error != nil
	}
	if !valid {
		return e, errors.New("invalid session event")
	}
	return e, nil
}

func validDownloadItem(item downloadItem) bool {
	if item.Bytes < 0 || (item.Total != nil && *item.Total < 0) {
		return false
	}
	switch item.Stage {
	case "queued", "transferring", "verifying", "publishing", "completed":
		return true
	}
	return false
}
