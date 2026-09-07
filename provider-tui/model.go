package main

import (
	"errors"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type sendFailure struct{ err error }
type enrollmentPoll struct{ revision int }

func checkEnrollmentLater(revision int) tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return enrollmentPoll{revision} })
}

type model struct {
	state          *snapshot
	selected       map[string]bool
	cursor         int
	bodyScroll     int
	expanded       bool
	width, height  int
	busy, quitting bool
	nextID         int
	problem        string
	progress       *progress
	code           *linkCode
	send           func(command) error
	stop           func()
	receive        func() backendMessage
}

func newModel(send func(command) error, receive func() backendMessage) *model {
	return &model{selected: map[string]bool{}, width: 80, height: 24, send: send, receive: receive, stop: func() {}}
}
func (m *model) Init() tea.Cmd      { return tea.Batch(m.action("hello", nil), m.nextEvent()) }
func (m *model) nextEvent() tea.Cmd { return func() tea.Msg { return m.receive() } }
func (m *model) action(action string, ids []string) tea.Cmd {
	m.nextID++
	m.busy = true
	m.problem = ""
	m.code = nil
	c := command{Version: protocolVersion, ID: m.nextID, Action: action, ModelIDs: ids}
	if m.state != nil {
		revision := m.state.Revision
		c.Revision = &revision
	}
	return func() tea.Msg {
		if err := m.send(c); err != nil {
			return sendFailure{err}
		}
		return nil
	}
}
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case enrollmentPoll:
		if !m.quitting && !m.busy && m.state != nil && m.state.Phase == "enrollment_pending" && m.state.Revision == msg.revision {
			return m, m.action("refresh", nil)
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case sendFailure:
		m.busy = false
		m.problem = "Could not send the action. Quit and reopen to resume."
	case backendMessage:
		if msg.err != nil {
			m.busy = false
			m.problem = msg.err.Error()
			// Keep the error visible until q; no invisible retry of side effects.
			return m, nil
		}
		e := msg.event
		switch e.Kind {
		case "snapshot":
			if m.state != nil && e.Snapshot.Revision <= m.state.Revision {
				return m, m.nextEvent()
			}
			if m.state == nil || m.state.Phase != e.Snapshot.Phase {
				m.bodyScroll = 0
			}
			m.state = e.Snapshot
			m.busy = m.state.Phase == "downloading"
			if m.state.Phase != "downloading" {
				m.progress = nil
				m.code = nil
			}
			if m.state.Phase == "models" {
				m.selected = map[string]bool{}
				for _, id := range m.state.SelectedModelIDs {
					m.selected[id] = true
				}
				for _, row := range m.state.Models {
					if m.selected[row.ID] && !row.Downloaded && row.FitReason != nil {
						m.expanded = true
					}
				}
				m.cursor = min(m.cursor, max(0, len(m.visible())-1))
			}
		case "progress":
			m.progress = e.Progress
		case "link_code":
			m.code = e.LinkCode
		case "error":
			m.busy = false
			m.problem = errorText(*e.Error)
		}
		if e.Kind == "snapshot" && m.state.Phase == "enrollment_pending" {
			return m, tea.Batch(m.nextEvent(), checkEnrollmentLater(m.state.Revision))
		}
		return m, m.nextEvent()
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c", "ctrl+d":
			m.quitting = true
			m.stop()
			return m, tea.Quit
		}
		if m.busy {
			return m, nil
		}
		if msg.String() == "r" {
			if m.state == nil {
				return m, m.action("hello", nil)
			}
			return m, m.action("refresh", nil)
		}
		if m.state == nil {
			return m, nil
		}
		if msg.String() == "pgdown" {
			m.bodyScroll += 3
		}
		if msg.String() == "pgup" {
			m.bodyScroll = max(0, m.bodyScroll-3)
		}
		rows := m.visible()
		if m.state.Phase == "enrollment_pending" && msg.String() == "o" {
			return m, m.action("enroll", nil)
		}
		if m.state.Phase == "models" {
			switch msg.String() {
			case "up", "k":
				m.cursor = max(0, m.cursor-1)
			case "down", "j":
				m.cursor = min(max(0, len(rows)-1), m.cursor+1)
			case "h":
				m.expanded = !m.expanded
				m.cursor = 0
			case " ":
				if len(rows) > 0 {
					id := rows[m.cursor].ID
					m.selected[id] = !m.selected[id]
				}
			}
		}
		if msg.String() == "enter" {
			switch m.state.Phase {
			case "enrollment":
				return m, m.action("enroll", nil)
			case "enrollment_pending":
				return m, m.action("refresh", nil)
			case "account":
				return m, m.action("link", nil)
			case "models":
				var ids []string
				for _, row := range m.state.Models {
					if m.selected[row.ID] {
						ids = append(ids, row.ID)
					}
				}
				if len(ids) == 0 {
					m.problem = "Select at least one model with Space."
					return m, nil
				}
				return m, m.action("select_models", ids)
			case "ready":
				return m, m.action("start", nil)
			case "started":
				m.quitting = true
				m.stop()
				return m, tea.Quit
			}
		}
	}
	return m, nil
}
func (m *model) visible() []catalogModel {
	if m.state == nil {
		return nil
	}
	rows := []catalogModel{}
	for group := 0; group < 3; group++ {
		for _, row := range m.state.Models {
			category := 1
			if row.Downloaded {
				category = 0
			} else if row.FitReason != nil {
				category = 2
			}
			if category == group && (group < 2 || m.expanded) {
				rows = append(rows, row)
			}
		}
	}
	return rows
}
func receiveFrom(b *backend) func() backendMessage {
	return func() backendMessage {
		msg, ok := <-b.messages
		if !ok {
			return backendMessage{err: errors.New("onboarding session closed; quit and reopen to resume")}
		}
		return msg
	}
}
