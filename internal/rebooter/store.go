package rebooter

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
)

type Entry struct {
	Device        config.Device   `json:"device"`
	Before        device.Snapshot `json:"before"`
	RestoreNeeded bool            `json:"restore_needed"`
}
type State struct {
	Version   int       `json:"version"`
	Created   time.Time `json:"created"`
	Submitted time.Time `json:"submitted,omitempty"`
	Stage     string    `json:"stage"`
	Updated   time.Time `json:"updated,omitzero"`
	Outcome   string    `json:"outcome"`
	GroupID   string    `json:"group_id"`
	APID      string    `json:"ap_id,omitempty"` // empty: the whole network
	Entries   []Entry   `json:"entries"`
}
type Store struct {
	dir  string
	lock *os.File
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "process.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	// The exclusive lock is held for the process lifetime, including recovery.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("state directory is already locked by another rebooter")
	}
	return &Store{dir: dir, lock: f}, nil
}
func (s *Store) Close() {
	if s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
	}
}
func (s *Store) Load() (*State, error) {
	f, err := os.Open(filepath.Join(s.dir, "operation.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var state State
	dec := json.NewDecoder(io.LimitReader(f, 2<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return nil, errors.New("invalid saved state; manual recovery required")
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid trailing saved state")
	}
	if state.Version != 1 || len(state.Entries) == 0 || state.Created.IsZero() {
		return nil, errors.New("unsupported/incomplete saved state")
	}
	switch state.Stage {
	case "quieting", "reboot_intent", "waiting", "restoring", "done":
	default:
		return nil, errors.New("unknown saved operation stage")
	}
	for _, entry := range state.Entries {
		if entry.RestoreNeeded && (entry.Device.Kind != "wifi_point" || entry.Before.Night == nil || entry.Before.Volume == nil) {
			return nil, errors.New("incomplete restoration snapshot")
		}
	}
	return &state, nil
}

// Save uses write+fsync+rename+directory fsync. Intent is durable before any POST.
func (s *Store) Save(state *State) error {
	state.Updated = time.Now()
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".operation-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.dir, "operation.json")); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// The schedule is on unless switched off from Home Assistant; the presence of
// this marker file survives restarts.
func (s *Store) scheduleOff() string { return filepath.Join(s.dir, "schedule-off") }

func (s *Store) ScheduleEnabled() bool {
	_, err := os.Stat(s.scheduleOff())
	return errors.Is(err, os.ErrNotExist)
}

func (s *Store) SetScheduleEnabled(on bool) error {
	if on {
		if err := os.Remove(s.scheduleOff()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return os.WriteFile(s.scheduleOff(), nil, 0o600)
}
