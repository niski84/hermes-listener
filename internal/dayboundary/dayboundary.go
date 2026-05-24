// Package dayboundary defines the project-wide "logical day" cutoff.
//
// The user's working day routinely runs past midnight. A naive
// time.Now().Format("2006-01-02") splits a single session that
// straddles 00:00 into two daily rollups, which is wrong. The
// logical day instead rolls over at DayCutoffHour (default 6 AM):
// any timestamp before 06:00:00 local time belongs to the previous
// calendar day.
//
// All daily-grouped artifacts (transcripts, session files, daily
// rollup notes, nutrition aggregations, mem0 fact dates) should be
// keyed off LogicalDate / LogicalDateString / Today rather than
// time.Now().Format("2006-01-02").
//
// The audio capture directory (data/audio/<YYYY-MM-DD>/) is the one
// intentional exception — it is keyed off clip.CapturedAt's
// calendar date because the recovery scanner walks the directory
// tree at startup and only knows wall-clock time. See
// internal/startup/audio.go.
package dayboundary

import (
	"os"
	"strconv"
	"time"
)

// DefaultCutoffHour is the hour-of-day (local) at which the logical
// day rolls over when DAY_CUTOFF_HOUR is not set.
const DefaultCutoffHour = 6

// Cutoff returns the configured logical-day cutoff hour (0–23).
// Reads DAY_CUTOFF_HOUR on every call so tests can override via
// t.Setenv. An invalid value falls back to DefaultCutoffHour.
func Cutoff() int {
	if v := os.Getenv("DAY_CUTOFF_HOUR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n < 24 {
			return n
		}
	}
	return DefaultCutoffHour
}

// LogicalDate returns the logical calendar day for t in t's location.
// Timestamps strictly before the cutoff hour are attributed to the
// previous calendar day. At exactly cutoff:00:00.000 the result is
// the current calendar day.
func LogicalDate(t time.Time) time.Time {
	shifted := t.Add(-time.Duration(Cutoff()) * time.Hour)
	return time.Date(shifted.Year(), shifted.Month(), shifted.Day(), 0, 0, 0, 0, shifted.Location())
}

// LogicalDateString returns the YYYY-MM-DD string of LogicalDate(t).
func LogicalDateString(t time.Time) string {
	return LogicalDate(t).Format("2006-01-02")
}

// Today returns LogicalDateString(time.Now()).
func Today() string {
	return LogicalDateString(time.Now())
}

// Yesterday returns the logical day before Today, as a YYYY-MM-DD string.
func Yesterday() string {
	return LogicalDate(time.Now()).AddDate(0, 0, -1).Format("2006-01-02")
}

// SQLDateExpr returns a SQLite date() expression that yields the logical
// date for a column of timestamps stored as ISO8601/RFC3339 strings.
// Example: SQLDateExpr("consumed_at") with cutoff=6 →
//
//	date(consumed_at, '-6 hours')
//
// Useful for grouping/filtering rows by logical day in queries.
func SQLDateExpr(column string) string {
	h := Cutoff()
	if h == 0 {
		return "date(" + column + ")"
	}
	return "date(" + column + ", '-" + strconv.Itoa(h) + " hours')"
}
