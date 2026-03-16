// Package manager implements the Manager component of the hersh framework.
// Manager handles state management through Reducer and Effect System.
package manager

import (
	"github.com/HershyOrg/watch/wm"
)

// SignalChannels holds all event channels for the Manager.
type SignalChannels struct {
	VarSigChan       chan *wm.DELETED_VarSig
	UserEventChan    chan *UserMessageReceived
	ControlEventChan chan *ControlEvent
	NewSigAppended   chan struct{} // Notifies when any event is added
}

// NewSignalChannels creates a new SignalChannels with buffered channels.
func NewSignalChannels(bufferSize int) *SignalChannels {
	return &SignalChannels{
		VarSigChan:       make(chan *wm.DELETED_VarSig, bufferSize),
		UserEventChan:    make(chan *UserMessageReceived, bufferSize),
		ControlEventChan: make(chan *ControlEvent, bufferSize),
		NewSigAppended:   make(chan struct{}, bufferSize*3),
	}
}

// SendVarSig sends a VarSig and notifies of new event.
func (sc *SignalChannels) SendVarSig(sig *wm.DELETED_VarSig) {
	sc.VarSigChan <- sig
	select {
	case sc.NewSigAppended <- struct{}{}:
	default:
	}
}

// SendUserEvent sends a UserMessageReceived and notifies of new event.
func (sc *SignalChannels) SendUserEvent(event *UserMessageReceived) {
	sc.UserEventChan <- event
	select {
	case sc.NewSigAppended <- struct{}{}:
	default:
	}
}

// SendControlEvent sends a ControlEvent and notifies of new event.
func (sc *SignalChannels) SendControlEvent(event *ControlEvent) {
	sc.ControlEventChan <- event
	select {
	case sc.NewSigAppended <- struct{}{}:
	default:
	}
}

// Close closes all event channels.
func (sc *SignalChannels) Close() {
	close(sc.VarSigChan)
	close(sc.UserEventChan)
	close(sc.ControlEventChan)
	close(sc.NewSigAppended)
}
