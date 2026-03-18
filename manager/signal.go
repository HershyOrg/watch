// Package manager implements the Manager component of the hersh framework.
// Manager handles state management through Reducer and Effect System.
package manager

// SignalChannels holds all event channels for the Manager.
type SignalChannels struct {
	UserEventChan    chan *UserMessageReceived
	ControlEventChan chan *ControlEvent
	NewSigAppended   chan struct{} // Notifies when any event is added (User/Control/WM notification)
}

// NewSignalChannels creates a new SignalChannels with buffered channels.
func NewSignalChannels(bufferSize int) *SignalChannels {
	return &SignalChannels{
		UserEventChan:    make(chan *UserMessageReceived, bufferSize),
		ControlEventChan: make(chan *ControlEvent, bufferSize),
		NewSigAppended:   make(chan struct{}, bufferSize*3),
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
	close(sc.UserEventChan)
	close(sc.ControlEventChan)
	close(sc.NewSigAppended)
}
