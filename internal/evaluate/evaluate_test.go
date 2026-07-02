package evaluate

import "testing"

func TestTransition(t *testing.T) {
	tests := []struct {
		name   string
		prevUp bool
		nowUp  bool
		want   Event
	}{
		{"up stays up", true, true, None},
		{"up goes down", true, false, Down},
		{"down comes back", false, true, Recovered},
		{"down stays down", false, false, None},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Transition(tt.prevUp, tt.nowUp); got != tt.want {
				t.Errorf("Transition(%v, %v) = %v, want %v", tt.prevUp, tt.nowUp, got, tt.want)
			}
		})
	}
}

// TestSequence walks a realistic outage: healthy, one bad check opens an
// incident, more bad checks are quiet, then recovery — asserting exactly one
// Down and one Recovered are emitted across the run.
func TestSequence(t *testing.T) {
	results := []bool{true, true, false, false, false, true, true}
	var downs, recoveries int
	prev := true // seed: assume up
	for _, up := range results {
		switch Transition(prev, up) {
		case Down:
			downs++
		case Recovered:
			recoveries++
		}
		prev = up
	}
	if downs != 1 || recoveries != 1 {
		t.Errorf("got %d downs / %d recoveries, want 1 / 1", downs, recoveries)
	}
}
