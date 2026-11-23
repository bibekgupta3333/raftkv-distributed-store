// Package observability holds RaftKV's logs, metrics and tracing helpers
// (D-17). It depends on the core only for types; the core never imports it.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// TermSource returns the node's current Raft term. The logger calls it for
// every record so the term is always current without the caller passing it.
type TermSource func() uint64

// ParseLevel maps "debug", "info", "warn" or "error" to a slog level.
func ParseLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, true
	case "info", "":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	}
	return slog.LevelInfo, false
}

// NewLogger returns a JSON slog logger writing to w. Every record carries
// node_id and, when term is non-nil, the current term (WBS 2.4.1). Give each
// subsystem its own logger with [Component].
func NewLogger(w io.Writer, level slog.Level, nodeID uint64, term TermSource) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(&termHandler{inner: base.WithAttrs([]slog.Attr{slog.Uint64("node_id", nodeID)}), term: term})
}

// Component returns l with a component attribute (for example "raft", "wal",
// "transport").
func Component(l *slog.Logger, name string) *slog.Logger { return l.With("component", name) }

// termHandler adds the live term to every record.
type termHandler struct {
	inner slog.Handler
	term  TermSource
}

func (h *termHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.inner.Enabled(ctx, l) }

func (h *termHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.term != nil {
		r.AddAttrs(slog.Uint64("term", h.term()))
	}
	return h.inner.Handle(ctx, r)
}

func (h *termHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &termHandler{inner: h.inner.WithAttrs(as), term: h.term}
}

func (h *termHandler) WithGroup(name string) slog.Handler {
	return &termHandler{inner: h.inner.WithGroup(name), term: h.term}
}
