package raftlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Snapshot persistence. A snapshot captures the state machine's state as of
// lastIncludedIndex/lastIncludedTerm, so every log entry at or below that index
// can be discarded. The file is written atomically (temp + fsync + rename +
// dir-fsync) so a crash never exposes a half-written snapshot.
//
// File layout (big-endian): index(8) term(8) dataLen(4) crc32(4) data(dataLen),
// where the CRC covers index || term || data.

const snapshotHeaderLen = 8 + 8 + 4 + 4

func snapshotPath(dir string) string { return filepath.Join(dir, "snapshot") }

// writeSnapshotFile atomically persists a snapshot.
func writeSnapshotFile(dir string, index, term uint64, data []byte) error {
	buf := make([]byte, snapshotHeaderLen+len(data))
	binary.BigEndian.PutUint64(buf[0:8], index)
	binary.BigEndian.PutUint64(buf[8:16], term)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(data)))
	// CRC over index || term || data (skip the length/crc header fields).
	h := crc32.New(crcTable)
	h.Write(buf[0:16])
	h.Write(data)
	binary.BigEndian.PutUint32(buf[20:24], h.Sum32())
	copy(buf[snapshotHeaderLen:], data)

	tmp := snapshotPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: open snapshot tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: write snapshot tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: sync snapshot tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raftlog: close snapshot tmp: %w", err)
	}
	if err := os.Rename(tmp, snapshotPath(dir)); err != nil {
		return fmt.Errorf("raftlog: rename snapshot: %w", err)
	}
	return fsyncDir(dir)
}

// readSnapshotFile loads the snapshot, returning ok=false if none exists.
func readSnapshotFile(dir string) (index, term uint64, data []byte, ok bool, err error) {
	b, err := os.ReadFile(snapshotPath(dir))
	if os.IsNotExist(err) {
		return 0, 0, nil, false, nil
	}
	if err != nil {
		return 0, 0, nil, false, fmt.Errorf("raftlog: read snapshot: %w", err)
	}
	if len(b) < snapshotHeaderLen {
		return 0, 0, nil, false, fmt.Errorf("raftlog: snapshot too short (%d bytes)", len(b))
	}
	index = binary.BigEndian.Uint64(b[0:8])
	term = binary.BigEndian.Uint64(b[8:16])
	dataLen := binary.BigEndian.Uint32(b[16:20])
	wantCRC := binary.BigEndian.Uint32(b[20:24])
	if int(dataLen) != len(b)-snapshotHeaderLen {
		return 0, 0, nil, false, fmt.Errorf("raftlog: snapshot data length mismatch")
	}
	payload := b[snapshotHeaderLen:]
	h := crc32.New(crcTable)
	h.Write(b[0:16])
	h.Write(payload)
	if h.Sum32() != wantCRC {
		return 0, 0, nil, false, fmt.Errorf("raftlog: snapshot crc mismatch")
	}
	data = append([]byte(nil), payload...)
	return index, term, data, true, nil
}
