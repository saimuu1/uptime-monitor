package raftlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// HardState is the Raft state that MUST be persisted before this node responds
// to any RPC that depends on it: the current term and the candidate it voted for
// in that term (0 = no vote yet). Losing this across a restart would let a node
// vote twice in one term or accept a stale leader — both break Raft safety, so
// it is written with an fsync and an atomic rename.
type HardState struct {
	Term     uint64
	VotedFor uint64 // 0 = no vote this term; node ids are 1-based
}

// hardStateLen: term(8) + votedFor(8) + crc32(4).
const hardStateLen = 8 + 8 + 4

// SaveHardState writes hs durably using write-temp-then-rename so a crash can
// never expose a half-written state file: a reader sees either the old complete
// file or the new complete one.
func SaveHardState(dir string, hs HardState) error {
	buf := make([]byte, hardStateLen)
	binary.BigEndian.PutUint64(buf[0:8], hs.Term)
	binary.BigEndian.PutUint64(buf[8:16], hs.VotedFor)
	binary.BigEndian.PutUint32(buf[16:20], crc32.Checksum(buf[0:16], crcTable))

	tmp := filepath.Join(dir, "hardstate.tmp")
	final := filepath.Join(dir, "hardstate")

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: open hardstate tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: write hardstate tmp: %w", err)
	}
	if err := f.Sync(); err != nil { // contents durable before the rename
		f.Close()
		return fmt.Errorf("raftlog: sync hardstate tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raftlog: close hardstate tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("raftlog: rename hardstate: %w", err)
	}
	return fsyncDir(dir) // make the rename itself durable
}

// LoadHardState reads the persisted state, returning the zero value if none
// exists yet (a fresh node starts at term 0 with no vote).
func LoadHardState(dir string) (HardState, error) {
	b, err := os.ReadFile(filepath.Join(dir, "hardstate"))
	if os.IsNotExist(err) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, fmt.Errorf("raftlog: read hardstate: %w", err)
	}
	if len(b) != hardStateLen {
		return HardState{}, fmt.Errorf("raftlog: hardstate wrong size (%d bytes)", len(b))
	}
	if got, want := crc32.Checksum(b[0:16], crcTable), binary.BigEndian.Uint32(b[16:20]); got != want {
		return HardState{}, fmt.Errorf("raftlog: hardstate crc mismatch")
	}
	return HardState{
		Term:     binary.BigEndian.Uint64(b[0:8]),
		VotedFor: binary.BigEndian.Uint64(b[8:16]),
	}, nil
}

// fsyncDir fsyncs a directory so a create/rename within it is durable (on POSIX
// the directory entry is a separate write from the file's contents).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raftlog: open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("raftlog: fsync dir: %w", err)
	}
	return nil
}
