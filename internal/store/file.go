package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// File is a Mem store persisted to <dir>/store.json, so the CLI, the local
// stream consumer and the local dashboard share one dataset across
// processes. Every operation takes an advisory lock, reloads the file if
// another process changed it, and (for writes) saves it back.
//
// It is for local development and demos, not for volume: the whole dataset
// is rewritten on every write.
type File struct {
	*Mem
	path  string
	lock  *os.File
	mtime time.Time
	size  int64
}

// OpenFile opens (or creates) a file store in dir.
func OpenFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(filepath.Join(dir, "store.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	f := &File{Mem: NewMem(), path: filepath.Join(dir, "store.json"), lock: lf}
	f.Mem.before = f.before
	f.Mem.after = f.after
	return f, nil
}

// Path is the data file.
func (f *File) Path() string { return f.path }

func (f *File) before(bool) error {
	if err := syscall.Flock(int(f.lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock store: %w", err)
	}
	st, err := os.Stat(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if st.ModTime().Equal(f.mtime) && st.Size() == f.size {
		return nil
	}
	b, err := os.ReadFile(f.path)
	if err != nil {
		return err
	}
	d := newMemData()
	if err := json.Unmarshal(b, d); err != nil {
		return fmt.Errorf("read %s: %w", f.path, err)
	}
	d.fill()
	f.Mem.d = d
	f.mtime, f.size = st.ModTime(), st.Size()
	return nil
}

func (f *File) after(write bool) error {
	defer syscall.Flock(int(f.lock.Fd()), syscall.LOCK_UN)
	if !write {
		return nil
	}
	b, err := json.Marshal(f.Mem.d)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	if st, err := os.Stat(f.path); err == nil {
		f.mtime, f.size = st.ModTime(), st.Size()
	}
	return nil
}

// Close releases the lock file.
func (f *File) Close() error { return f.lock.Close() }
