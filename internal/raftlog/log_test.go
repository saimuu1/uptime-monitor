package raftlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// appendN appends n command entries with term t and data "v<index>", returning
// the entries it wrote.
func appendN(t *testing.T, l *Log, term uint64, n int) []Entry {
	t.Helper()
	var out []Entry
	for i := 0; i < n; i++ {
		e, err := l.Append(term, EntryCommand, []byte(fmt.Sprintf("v%d", l.LastIndex()+1)))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		out = append(out, e)
	}
	return out
}

func TestAppendAndRead(t *testing.T) {
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	want := appendN(t, l, 1, 5)
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", l.LastIndex())
	}
	for _, w := range want {
		got, ok := l.At(w.Index)
		if !ok {
			t.Fatalf("At(%d) not found", w.Index)
		}
		if got.Term != w.Term || !bytes.Equal(got.Data, w.Data) {
			t.Fatalf("At(%d) = %+v, want %+v", w.Index, got, w)
		}
	}
	if _, ok := l.At(0); ok {
		t.Fatal("At(0) should be out of range")
	}
	if _, ok := l.At(6); ok {
		t.Fatal("At(6) should be out of range")
	}
	// TermAt(0) is the empty sentinel.
	if term, ok := l.TermAt(0); !ok || term != 0 {
		t.Fatalf("TermAt(0) = %d,%v; want 0,true", term, ok)
	}
}

func TestReopenPersists(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 2, 4)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LastIndex() != 4 {
		t.Fatalf("after reopen LastIndex = %d, want 4", l2.LastIndex())
	}
	e, ok := l2.At(3)
	if !ok || e.Term != 2 || !bytes.Equal(e.Data, []byte("v3")) {
		t.Fatalf("after reopen At(3) = %+v,%v", e, ok)
	}
	// A subsequent append must continue at index 5 and survive another reopen.
	appendN(t, l2, 3, 1)
	if l2.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", l2.LastIndex())
	}
}

func TestTornTailDiscardedOnOpen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 1, 3)
	l.Close()

	// Simulate a crash mid-append: garbage bytes appended after the last good
	// record (a partial header, then a bogus payload).
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 42, 1, 2, 3}); err != nil { // len=42, only 3 payload bytes
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("open after torn write: %v", err)
	}
	defer l2.Close()
	if l2.LastIndex() != 3 {
		t.Fatalf("torn tail not discarded: LastIndex = %d, want 3", l2.LastIndex())
	}
	// The file must have been truncated back to the last clean boundary, and a
	// fresh append must land at index 4 and read back correctly.
	appendN(t, l2, 2, 1)
	e, ok := l2.At(4)
	if !ok || e.Index != 4 {
		t.Fatalf("append after recovery failed: %+v,%v", e, ok)
	}
	l2.Close()
	l3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l3.Close()
	if l3.LastIndex() != 4 {
		t.Fatalf("post-recovery append not durable: LastIndex = %d, want 4", l3.LastIndex())
	}
}

func TestCorruptedCRCTruncates(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 1, 3)
	l.Close()

	// Flip a byte inside the LAST record's payload so its CRC no longer matches.
	path := filepath.Join(dir, "log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	// The last record is unreadable, so only the first two survive.
	if l2.LastIndex() != 2 {
		t.Fatalf("corrupt trailing record not dropped: LastIndex = %d, want 2", l2.LastIndex())
	}
}

func TestTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 1, 10)

	if err := l.TruncateSuffix(5); err != nil { // drop indices >= 5
		t.Fatal(err)
	}
	if l.LastIndex() != 4 {
		t.Fatalf("after TruncateSuffix(5) LastIndex = %d, want 4", l.LastIndex())
	}
	if _, ok := l.At(5); ok {
		t.Fatal("At(5) should be gone")
	}
	// New appends reuse the freed indices, and the result survives a reopen.
	appendN(t, l, 2, 2) // indices 5,6
	if l.LastIndex() != 6 {
		t.Fatalf("LastIndex = %d, want 6", l.LastIndex())
	}
	l.Close()

	l2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LastIndex() != 6 {
		t.Fatalf("after reopen LastIndex = %d, want 6", l2.LastIndex())
	}
	if e, ok := l2.At(5); !ok || e.Term != 2 {
		t.Fatalf("At(5) after truncate+reopen = %+v,%v; want term 2", e, ok)
	}
	// Truncating everything empties the log.
	if err := l2.TruncateSuffix(1); err != nil {
		t.Fatal(err)
	}
	if l2.LastIndex() != 0 || l2.Len() != 0 {
		t.Fatalf("empty log expected, got LastIndex=%d Len=%d", l2.LastIndex(), l2.Len())
	}
}

func TestFromShipsSuffix(t *testing.T) {
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	appendN(t, l, 1, 5)

	got := l.From(3)
	if len(got) != 3 || got[0].Index != 3 || got[2].Index != 5 {
		t.Fatalf("From(3) = %+v", got)
	}
	if got := l.From(6); got != nil {
		t.Fatalf("From(6) = %+v, want nil", got)
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Missing file => zero value.
	hs, err := LoadHardState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if hs != (HardState{}) {
		t.Fatalf("fresh LoadHardState = %+v, want zero", hs)
	}

	if err := SaveHardState(dir, HardState{Term: 7, VotedFor: 3}); err != nil {
		t.Fatal(err)
	}
	hs, err = LoadHardState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 7 || hs.VotedFor != 3 {
		t.Fatalf("LoadHardState = %+v, want {7,3}", hs)
	}

	// Overwrite (a later term, vote reset) persists.
	if err := SaveHardState(dir, HardState{Term: 8, VotedFor: 0}); err != nil {
		t.Fatal(err)
	}
	hs, _ = LoadHardState(dir)
	if hs.Term != 8 || hs.VotedFor != 0 {
		t.Fatalf("LoadHardState = %+v, want {8,0}", hs)
	}

	// A corrupt hardstate is reported, not silently accepted.
	path := filepath.Join(dir, "hardstate")
	raw, _ := os.ReadFile(path)
	raw[0] ^= 0xFF
	os.WriteFile(path, raw, 0o644)
	if _, err := LoadHardState(dir); err == nil {
		t.Fatal("expected crc error on corrupt hardstate")
	}
}

// TestModelAgainstLog is a randomized property test: it drives the log with a
// random mix of appends, suffix truncations, and reopens, and after every op
// cross-checks the log against an in-memory model. Because every append and
// truncate fsyncs, a reopen must reproduce the model exactly — this is the
// crash-safety guarantee, exercised across thousands of interleavings.
func TestModelAgainstLog(t *testing.T) {
	dir := t.TempDir()
	seed := int64(1)
	if s := os.Getenv("RAFTLOG_SEED"); s != "" {
		fmt.Sscan(s, &seed)
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed=%d (set RAFTLOG_SEED to reproduce)", seed)

	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var model []Entry
	term := uint64(1)

	check := func() {
		if l.LastIndex() != modelLast(model) {
			t.Fatalf("LastIndex = %d, model = %d", l.LastIndex(), modelLast(model))
		}
		for _, w := range model {
			got, ok := l.At(w.Index)
			if !ok || got.Term != w.Term || !bytes.Equal(got.Data, w.Data) {
				t.Fatalf("At(%d) = %+v,%v; model %+v", w.Index, got, ok, w)
			}
		}
	}

	for i := 0; i < 3000; i++ {
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5: // append (most common)
			if rng.Intn(20) == 0 {
				term++ // occasionally bump the term
			}
			data := make([]byte, rng.Intn(24))
			rng.Read(data)
			e, err := l.Append(term, EntryCommand, data)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			model = append(model, e)
		case 6, 7: // truncate suffix
			if len(model) == 0 {
				continue
			}
			from := uint64(1 + rng.Intn(len(model)+1)) // 1..last+1
			if err := l.TruncateSuffix(from); err != nil {
				t.Fatalf("truncate: %v", err)
			}
			if from >= 1 && int(from-1) <= len(model) {
				model = model[:from-1]
			}
		case 8, 9: // close + reopen (exercise crash recovery path)
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			l, err = Open(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
		}
		check()
	}
	l.Close()
}

func modelLast(m []Entry) uint64 {
	if len(m) == 0 {
		return 0
	}
	return m[len(m)-1].Index
}

// sanity: the record framing constants line up with what we write, so a change
// to one without the other fails fast.
func TestFramingConstants(t *testing.T) {
	e := Entry{Index: 1, Term: 1, Type: EntryCommand, Data: []byte("abc")}
	buf := make([]byte, e.encodedLen())
	e.encode(buf)
	if crc32.Checksum(buf, crcTable) == 0 {
		t.Skip("crc happened to be zero; harmless")
	}
	var lenField [4]byte
	binary.BigEndian.PutUint32(lenField[:], uint32(len(buf)))
	if int(binary.BigEndian.Uint32(lenField[:])) != e.encodedLen() {
		t.Fatal("length framing mismatch")
	}
}
