// Package evaluate holds the up/down state machine. In v1 it's a single pure
// function; v3 grows it into region consensus and flap suppression. Keeping it
// pure (no I/O) means the transition logic is trivially unit-testable.
package evaluate

// Event is what a new check result means relative to the previous state.
type Event int

const (
	// None: state unchanged (up→up or down→down).
	None Event = iota
	// Down: the monitor just went from up to down — open an incident + alert.
	Down
	// Recovered: the monitor just came back — resolve the incident + alert.
	Recovered
)

func (e Event) String() string {
	switch e {
	case Down:
		return "DOWN"
	case Recovered:
		return "RECOVERED"
	default:
		return "NONE"
	}
}

// Transition compares the previous up-state to the newly-observed one and
// reports the resulting event.
func Transition(prevUp, nowUp bool) Event {
	switch {
	case prevUp && !nowUp:
		return Down
	case !prevUp && nowUp:
		return Recovered
	default:
		return None
	}
}
