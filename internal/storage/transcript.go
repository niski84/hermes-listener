package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"hermes-listener/internal/dayboundary"
)

// DailyTranscript appends timestamped transcript lines to a rolling daily file.
type DailyTranscript struct {
	dir string
	mu  sync.Mutex
}

func NewDailyTranscript(dir string) (*DailyTranscript, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create transcript dir: %w", err)
	}
	return &DailyTranscript{dir: dir}, nil
}

func (dt *DailyTranscript) Append(text string, at time.Time) error {
	return dt.AppendTagged(text, "", at)
}

// AppendTagged writes a transcript line with an optional source tag.
// When source is non-empty the line reads: [HH:MM:SS] [Source] text
// This lets the summarizer attribute speech to the correct input channel.
func (dt *DailyTranscript) AppendTagged(text, source string, at time.Time) error {
	if text == "" {
		return nil
	}
	dt.mu.Lock()
	defer dt.mu.Unlock()

	path := dt.pathFor(at)

	// Write date header when creating a new file.
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open transcript: %w", err)
	}
	defer f.Close()

	if os.IsNotExist(statErr) {
		// Header reflects the LOGICAL day this file belongs to, not the
		// wall-clock date — a 02:00 line creating yesterday's file
		// must still read "Monday, January 9 2026" not "January 10".
		fmt.Fprintf(f, "# Transcript — %s\n\n", dayboundary.LogicalDate(at).Format("Monday, January 2 2006"))
	}

	if source != "" {
		_, err = fmt.Fprintf(f, "[%s] [%s] %s\n", at.Format("15:04:05"), source, text)
	} else {
		_, err = fmt.Fprintf(f, "[%s] %s\n", at.Format("15:04:05"), text)
	}
	return err
}

// ReadToday returns the full content of today's transcript.
func (dt *DailyTranscript) ReadToday() (string, error) {
	return dt.ReadDate(time.Now())
}

// ReadTodayForChannel returns today's transcript filtered to lines tagged
// with channelID. Header / blank lines are preserved.
//
// Backward-compat: untagged lines (written before fix E) are included
// when channelID == "default" — the default mic was historically the
// only writer — and excluded for any other channel.
func (dt *DailyTranscript) ReadTodayForChannel(channelID string) (string, error) {
	full, err := dt.ReadToday()
	if err != nil {
		return "", err
	}
	if channelID == "" {
		return full, nil
	}
	return filterTranscriptByChannel(full, channelID), nil
}

// taggedLineRE matches a tagged transcript line: [HH:MM:SS] [<tag>] rest
// where <tag> may be any non-bracket characters.
var taggedLineRE = regexp.MustCompile(`^\[\d\d:\d\d:\d\d\] \[([^\]]+)\] `)

// timestampedLineRE matches the legacy untagged shape: [HH:MM:SS] rest
var timestampedLineRE = regexp.MustCompile(`^\[\d\d:\d\d:\d\d\] `)

// filterTranscriptByChannel keeps:
//   - lines tagged exactly with channelID
//   - non-content lines (header, blank lines)
//   - untagged timestamped lines IF channelID == "default" (legacy compat)
func filterTranscriptByChannel(text, channelID string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		// Pass through headers / blank lines.
		if line == "" || strings.HasPrefix(line, "# ") {
			out = append(out, line)
			continue
		}
		if m := taggedLineRE.FindStringSubmatch(line); m != nil {
			if m[1] == channelID {
				out = append(out, line)
			}
			continue
		}
		// Untagged timestamped line — legacy.
		if timestampedLineRE.MatchString(line) {
			if channelID == "default" {
				out = append(out, line)
			}
			continue
		}
		// Non-timestamped narrative lines (rare) — keep.
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// ReadDate returns the transcript for a given day.
func (dt *DailyTranscript) ReadDate(t time.Time) (string, error) {
	data, err := os.ReadFile(dt.pathFor(t))
	if os.IsNotExist(err) {
		return "", nil
	}
	return string(data), err
}

// TodayPath is the file path for today's transcript.
func (dt *DailyTranscript) TodayPath() string {
	return dt.pathFor(time.Now())
}

// pathFor returns the on-disk file for the logical day that contains t.
// A 02:30 timestamp on May 10 maps to the "2026-05-09-transcript.md"
// file because the user's logical day runs 06:00 → 06:00.
func (dt *DailyTranscript) pathFor(t time.Time) string {
	return filepath.Join(dt.dir, fmt.Sprintf("%s-transcript.md", dayboundary.LogicalDateString(t)))
}
