package dayboundary

import (
	"testing"
	"time"
)

// mustLoc is a tiny helper to keep table tests readable.
func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return loc
}

func TestLogicalDate_BeforeCutoff_ReturnsPreviousDay(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	in := time.Date(2026, 5, 10, 2, 30, 0, 0, loc)
	got := LogicalDateString(in)
	if got != "2026-05-09" {
		t.Fatalf("02:30 on May 10 → got %s, want 2026-05-09", got)
	}
}

func TestLogicalDate_JustBeforeCutoff_ReturnsPreviousDay(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	in := time.Date(2026, 5, 10, 5, 59, 59, 999_000_000, loc)
	if got := LogicalDateString(in); got != "2026-05-09" {
		t.Fatalf("05:59:59.999 on May 10 → got %s, want 2026-05-09", got)
	}
}

func TestLogicalDate_AtCutoff_ReturnsToday(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	in := time.Date(2026, 5, 10, 6, 0, 0, 0, loc)
	if got := LogicalDateString(in); got != "2026-05-10" {
		t.Fatalf("06:00:00 on May 10 → got %s, want 2026-05-10", got)
	}
}

func TestLogicalDate_AfterCutoff_ReturnsToday(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	in := time.Date(2026, 5, 10, 14, 0, 0, 0, loc)
	if got := LogicalDateString(in); got != "2026-05-10" {
		t.Fatalf("14:00 on May 10 → got %s, want 2026-05-10", got)
	}
}

func TestLogicalDate_NearMidnight_ReturnsSameDay(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	in := time.Date(2026, 5, 10, 23, 59, 59, 0, loc)
	if got := LogicalDateString(in); got != "2026-05-10" {
		t.Fatalf("23:59 on May 10 → got %s, want 2026-05-10", got)
	}
}

func TestLogicalDate_AcrossMidnight_StaysOnPriorDay(t *testing.T) {
	loc := mustLoc(t, "America/Los_Angeles")
	// A session crossing 23:50 → 01:30 must produce the same logical date.
	startDate := LogicalDateString(time.Date(2026, 5, 10, 23, 50, 0, 0, loc))
	endDate := LogicalDateString(time.Date(2026, 5, 11, 1, 30, 0, 0, loc))
	if startDate != endDate {
		t.Fatalf("session straddling midnight split: start=%s end=%s", startDate, endDate)
	}
	if startDate != "2026-05-10" {
		t.Fatalf("expected 2026-05-10, got %s", startDate)
	}
}

func TestLogicalDate_PreservesTimezone(t *testing.T) {
	loc := mustLoc(t, "Asia/Tokyo")
	in := time.Date(2026, 5, 10, 3, 0, 0, 0, loc)
	got := LogicalDate(in)
	if got.Location().String() != loc.String() {
		t.Fatalf("location not preserved: got %s want %s", got.Location(), loc)
	}
	if got.Format("2006-01-02") != "2026-05-09" {
		t.Fatalf("Tokyo 03:00 May 10 → got %s, want 2026-05-09", got.Format("2006-01-02"))
	}
}

func TestCutoff_DefaultIsSix(t *testing.T) {
	t.Setenv("DAY_CUTOFF_HOUR", "")
	if got := Cutoff(); got != 6 {
		t.Fatalf("default cutoff: got %d, want 6", got)
	}
}

func TestCutoff_EnvOverride(t *testing.T) {
	t.Setenv("DAY_CUTOFF_HOUR", "4")
	if got := Cutoff(); got != 4 {
		t.Fatalf("override cutoff: got %d, want 4", got)
	}
	loc := mustLoc(t, "America/Los_Angeles")
	// With cutoff=4, 03:59 belongs to prior day, 04:00 to current day.
	if d := LogicalDateString(time.Date(2026, 5, 10, 3, 59, 0, 0, loc)); d != "2026-05-09" {
		t.Fatalf("cutoff=4, 03:59 → got %s, want 2026-05-09", d)
	}
	if d := LogicalDateString(time.Date(2026, 5, 10, 4, 0, 0, 0, loc)); d != "2026-05-10" {
		t.Fatalf("cutoff=4, 04:00 → got %s, want 2026-05-10", d)
	}
}

func TestCutoff_InvalidEnvFallsBack(t *testing.T) {
	t.Setenv("DAY_CUTOFF_HOUR", "garbage")
	if got := Cutoff(); got != 6 {
		t.Fatalf("invalid env: got %d, want default 6", got)
	}
	t.Setenv("DAY_CUTOFF_HOUR", "99")
	if got := Cutoff(); got != 6 {
		t.Fatalf("out-of-range env: got %d, want default 6", got)
	}
}

func TestSQLDateExpr(t *testing.T) {
	t.Setenv("DAY_CUTOFF_HOUR", "6")
	if got := SQLDateExpr("consumed_at"); got != "date(consumed_at, '-6 hours')" {
		t.Fatalf("got %q", got)
	}
	t.Setenv("DAY_CUTOFF_HOUR", "0")
	if got := SQLDateExpr("consumed_at"); got != "date(consumed_at)" {
		t.Fatalf("cutoff=0: got %q", got)
	}
}
