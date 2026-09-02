package raftlog

import (
	"bytes"
	"testing"
)

func mustAppendN(t *testing.T, l *Log, term uint64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := l.Append(term, EntryCommand, []byte{byte('a' + i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

func TestSnapshotCompacts(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppendN(t, l, 3, 10)
	term5, _ := l.TermAt(5)

	if err := l.Snapshot(5, term5, []byte("state@5")); err != nil {
		t.Fatal(err)
	}
	if l.SnapshotIndex() != 5 || l.SnapshotTerm() != term5 {
		t.Fatalf("snapshot meta = (%d,%d), want (5,%d)", l.SnapshotIndex(), l.SnapshotTerm(), term5)
	}
	if l.LastIndex() != 10 {
		t.Fatalf("LastIndex = %d, want 10", l.LastIndex())
	}
	if _, ok := l.At(3); ok {
		t.Fatal("At(3) should be compacted away")
	}
	if e, ok := l.At(6); !ok || e.Index != 6 {
		t.Fatalf("At(6) = %+v,%v", e, ok)
	}
	// The snapshot boundary still resolves for the consistency check.
	if term, ok := l.TermAt(5); !ok || term != term5 {
		t.Fatalf("TermAt(5) = %d,%v; want %d,true", term, ok, term5)
	}
	if _, ok := l.TermAt(4); ok {
		t.Fatal("TermAt(4) should be gone")
	}
	if got := l.From(1); len(got) != 5 || got[0].Index != 6 || got[4].Index != 10 {
		t.Fatalf("From(1) after snapshot = %+v", got)
	}
	// Appends continue at 11.
	mustAppendN(t, l, 3, 1)
	if l.LastIndex() != 11 {
		t.Fatalf("append after snapshot: LastIndex = %d, want 11", l.LastIndex())
	}

	// Reopen: snapshot metadata, data, and the surviving suffix persist.
	l.Close()
	l2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.SnapshotIndex() != 5 || l2.LastIndex() != 11 {
		t.Fatalf("after reopen snap=%d last=%d, want 5/11", l2.SnapshotIndex(), l2.LastIndex())
	}
	if !bytes.Equal(l2.SnapshotData(), []byte("state@5")) {
		t.Fatalf("snapshot data lost: %q", l2.SnapshotData())
	}
	if e, ok := l2.At(6); !ok || e.Index != 6 {
		t.Fatalf("At(6) after reopen = %+v,%v", e, ok)
	}
}

func TestInstallSnapshotKeepSuffix(t *testing.T) {
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	mustAppendN(t, l, 2, 8) // indices 1..8, all term 2
	term5, _ := l.TermAt(5)

	// Snapshot matches our entry at index 5 => keep 6..8.
	if err := l.InstallSnapshot(5, term5, []byte("s")); err != nil {
		t.Fatal(err)
	}
	if l.SnapshotIndex() != 5 || l.LastIndex() != 8 {
		t.Fatalf("snap=%d last=%d, want 5/8", l.SnapshotIndex(), l.LastIndex())
	}
	if e, ok := l.At(6); !ok || e.Index != 6 {
		t.Fatalf("At(6) = %+v,%v", e, ok)
	}
	if _, ok := l.At(5); ok {
		t.Fatal("At(5) should be compacted")
	}
}

func TestInstallSnapshotDiscardAll(t *testing.T) {
	// Case A: snapshot far beyond our log => discard everything, reseed.
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	mustAppendN(t, l, 1, 3)
	if err := l.InstallSnapshot(10, 5, []byte("far")); err != nil {
		t.Fatal(err)
	}
	if l.SnapshotIndex() != 10 || l.LastIndex() != 10 || l.Len() != 0 {
		t.Fatalf("discard-all wrong: snap=%d last=%d len=%d", l.SnapshotIndex(), l.LastIndex(), l.Len())
	}
	// Next append continues at 11.
	if _, err := l.Append(6, EntryCommand, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if e, ok := l.At(11); !ok || e.Index != 11 {
		t.Fatalf("append after reseed = %+v,%v", e, ok)
	}

	// Case B: index within the log but a conflicting term => discard all.
	l2, _ := Open(t.TempDir())
	defer l2.Close()
	mustAppendN(t, l2, 2, 8)
	if err := l2.InstallSnapshot(5, 9 /* term mismatch */, []byte("s")); err != nil {
		t.Fatal(err)
	}
	if l2.SnapshotIndex() != 5 || l2.LastIndex() != 5 || l2.Len() != 0 {
		t.Fatalf("conflict discard-all wrong: snap=%d last=%d len=%d", l2.SnapshotIndex(), l2.LastIndex(), l2.Len())
	}
}

// TestCrashMidCompaction simulates a crash after a snapshot file was written but
// before the log file was compacted: on open, the stale prefix must be skipped,
// the suffix retained, and the file rewritten into canonical form.
func TestCrashMidCompaction(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppendN(t, l, 1, 8) // indices 1..8
	l.Close()

	// Write the snapshot metadata as if compaction had started (index 4), but
	// leave the log file untouched (still holds 1..8).
	if err := writeSnapshotFile(dir, 4, 1, []byte("snap@4")); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("open after crashed compaction: %v", err)
	}
	defer l2.Close()
	if l2.SnapshotIndex() != 4 || l2.LastIndex() != 8 {
		t.Fatalf("recovery wrong: snap=%d last=%d, want 4/8", l2.SnapshotIndex(), l2.LastIndex())
	}
	if _, ok := l2.At(4); ok {
		t.Fatal("At(4) should have been compacted on recovery")
	}
	if e, ok := l2.At(5); !ok || e.Index != 5 {
		t.Fatalf("At(5) = %+v,%v", e, ok)
	}
	if got := l2.From(1); len(got) != 4 || got[0].Index != 5 {
		t.Fatalf("From(1) = %+v", got)
	}
	// The file was rewritten to canonical form, so a fresh reopen needs no skip.
	l2.Close()
	l3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l3.Close()
	if l3.SnapshotIndex() != 4 || l3.LastIndex() != 8 {
		t.Fatalf("second reopen wrong: snap=%d last=%d", l3.SnapshotIndex(), l3.LastIndex())
	}
}
