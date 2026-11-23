package command

import (
	"errors"
	"time"
)

// Session rules (D-09, api-design 5).
const (
	// SessionTTL is how long a session lives without activity, measured in
	// leader-stamped time (D-27), never in a replica's own clock.
	SessionTTL = 10 * time.Minute
	// SessionWindow is the most sequences a session may have outstanding
	// (sequence - acked_sequence).
	SessionWindow = 64
)

// Session outcomes. They are decided at apply time, identically on every
// replica (or before proposing, for the static checks), and the server maps
// them to FAILED_PRECONDITION. None of them changes user data.
var (
	ErrSessionExpired = errors.New("command: session unknown or expired")
	ErrSequenceStale  = errors.New("command: sequence at or below the acked sequence")
	ErrSessionWindow  = errors.New("command: too many outstanding sequences in the session")
)
