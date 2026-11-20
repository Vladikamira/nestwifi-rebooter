package rebooter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeLocal struct {
	snapshots          map[string]device.Snapshot
	writes             int
	failQuiet          string
	failRestore        string
	refuseVerification bool
	inspectFail        bool
	ignoreVolumeCap    bool
}

func (l *fakeLocal) Inspect(_ context.Context, d config.Device, _ bool) (device.Snapshot, error) {
	if l.inspectFail {
		return device.Snapshot{}, errors.New("offline")
	}
	s := l.snapshots[d.Name]
	if l.refuseVerification && l.writes > 0 && s.Night != nil {
		n := *s.Night
		n.Enabled = false
		s.Night = &n
	}
	return s, nil
}
func (l *fakeLocal) SetNight(_ context.Context, d config.Device, n device.Night) error {
	l.writes++
	if (n.Enabled && d.Name == l.failQuiet) || (!n.Enabled && d.Name == l.failRestore) {
		return errors.New("test write failed")
	}
	s := l.snapshots[d.Name]
	s.Night = &n
	if n.Enabled && n.Volume == 0 && !l.ignoreVolumeCap {
		s.Volume = &device.Volume{Level: 0}
	}
	l.snapshots[d.Name] = s
	return nil
}
func (l *fakeLocal) RestoreVolume(_ context.Context, d config.Device, v device.Volume) error {
	s := l.snapshots[d.Name]
	s.Volume = &v
	l.snapshots[d.Name] = s
	return nil
}

type fakeCloud struct {
	local            *fakeLocal
	calls, refreshes int
	apCalls          int
	err              error
	preflightErr     error
	doBoot           bool
}

func (c *fakeCloud) CheckGroup(context.Context, string, []string) error { return c.preflightErr }

// Test AP IDs: "ap-<device name>"; IPs as in fixture.
var fakeAPs = map[string][2]string{"ap-kitchen": {"192.0.2.2", "kitchen"}, "ap-router": {"192.0.2.1", "router"}}

func (c *fakeCloud) AccessPointIP(_ context.Context, _, apID string) (string, error) {
	if ap, ok := fakeAPs[apID]; ok {
		return ap[0], nil
	}
	return "", errors.New("unknown AP")
}
func (c *fakeCloud) RebootAP(_ context.Context, apID string) error {
	c.apCalls++
	name := fakeAPs[apID][1]
	s := c.local.snapshots[name]
	s.Boot, s.Uptime = s.Boot.Add(time.Hour), 1
	c.local.snapshots[name] = s
	return nil
}
func (c *fakeCloud) Refresh(context.Context, config.Device) error { c.refreshes++; return nil }
func (c *fakeCloud) Reboot(context.Context, string) error {
	c.calls++
	if c.doBoot {
		for name, s := range c.local.snapshots {
			s.Boot = s.Boot.Add(time.Hour) // a distinct new boot, even for back-to-back reboots
			s.Uptime = 1
			c.local.snapshots[name] = s
		}
	}
	return c.err
}

func fixture(t *testing.T, enabled bool) (*Runner, *fakeCloud, *fakeLocal) {
	t.Helper()
	var cfg config.Config
	cfg.Devices = []config.Device{{Name: "router", Kind: "router", IP: "192.0.2.1"}, {Name: "kitchen", CloudName: "Kitchen Wifi", Kind: "wifi_point", IP: "192.0.2.2"}, {Name: "office", CloudName: "Office Wifi", Kind: "wifi_point", IP: "192.0.2.3"}}
	cfg.Rebooter.GroupID = "group"
	cfg.Rebooter.WaitTimeout = 2 * time.Second
	cfg.Rebooter.PollInterval = time.Millisecond
	local := &fakeLocal{snapshots: map[string]device.Snapshot{}}
	for _, d := range cfg.Devices {
		s := device.Snapshot{At: time.Now(), Uptime: 3600, Boot: time.Now().Add(-time.Hour)}
		if d.Kind == "wifi_point" {
			s.Night = &device.Night{Brightness: 1, Volume: 1, Windows: []device.Window{{Days: []int{0}, Start: 22, Length: 8}}}
			s.Volume = &device.Volume{Level: 0.58}
		}
		local.snapshots[d.Name] = s
	}
	cloud := &fakeCloud{local: local, doBoot: true}
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	r := New(cfg, store, cloud, local, enabled, prometheus.NewRegistry())
	return r, cloud, local
}
func assertRestored(t *testing.T, r *Runner, l *fakeLocal) {
	t.Helper()
	s, err := r.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if s == nil || s.Stage != "done" {
		t.Fatalf("not done: %+v", s)
	}
	for _, entry := range s.Entries {
		if entry.RestoreNeeded {
			t.Fatal("pending restore")
		}
		if entry.Device.Kind == "wifi_point" {
			got := l.snapshots[entry.Device.Name]
			if !device.NightEqual(*got.Night, *entry.Before.Night) || !device.VolumeEqual(*got.Volume, *entry.Before.Volume) {
				t.Fatal("original settings lost")
			}
		}
	}
}
func TestSuccessfulRun(t *testing.T) {
	r, c, l := fixture(t, true)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 || c.refreshes != 2 {
		t.Fatalf("reboots=%d refreshes=%d", c.calls, c.refreshes)
	}
	assertRestored(t, r, l)
}
func TestDryRunNeverMutates(t *testing.T) {
	r, c, l := fixture(t, false)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 0 || l.writes != 0 {
		t.Fatal("dry run mutated devices")
	}
	s, _ := r.store.Load()
	if s != nil {
		t.Fatal("dry run saved operation")
	}
}
func TestPreflightFailureNeverMutates(t *testing.T) {
	r, c, l := fixture(t, true)
	c.preflightErr = errors.New("inventory mismatch")
	if r.Run(context.Background()) == nil {
		t.Fatal("expected error")
	}
	if c.calls != 0 || l.writes != 0 {
		t.Fatal("mutated after bad preflight")
	}
}
func TestOfflineDeviceAbortsBeforeQuiet(t *testing.T) {
	r, c, l := fixture(t, true)
	l.inspectFail = true
	if r.Run(context.Background()) == nil {
		t.Fatal("expected error")
	}
	if c.calls != 0 || l.writes != 0 {
		t.Fatal("mutated offline network")
	}
}
func TestQuietFailureRollsBackAndNeverReboots(t *testing.T) {
	r, c, l := fixture(t, true)
	l.failQuiet = "office"
	if r.Run(context.Background()) == nil {
		t.Fatal("expected quiet error")
	}
	if c.calls != 0 {
		t.Fatal("reboot after quiet failure")
	}
	assertRestored(t, r, l)
}

func TestNightModeWithoutActualVolumeCapBlocksReboot(t *testing.T) {
	r, c, l := fixture(t, true)
	l.ignoreVolumeCap = true
	if r.Run(context.Background()) == nil {
		t.Fatal("expected missing volume cap error")
	}
	if c.calls != 0 {
		t.Fatal("reboot with nonzero effective volume")
	}
	assertRestored(t, r, l)
}
func TestFinalQuietVerificationBlocksReboot(t *testing.T) {
	r, c, l := fixture(t, true)
	l.refuseVerification = true
	if r.Run(context.Background()) == nil {
		t.Fatal("expected verification error")
	}
	if c.calls != 0 {
		t.Fatal("reboot despite changed quiet settings")
	}
	assertRestored(t, r, l)
}
func TestAmbiguousCloudResponseNotRetried(t *testing.T) {
	r, c, l := fixture(t, true)
	c.err = errors.New("response lost")
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("retried reboot")
	}
	assertRestored(t, r, l)
}
func TestBootTimeoutStillRestores(t *testing.T) {
	r, c, l := fixture(t, true)
	r.c.Rebooter.WaitTimeout = 20 * time.Millisecond
	c.doBoot = false
	if r.Run(context.Background()) == nil {
		t.Fatal("expected boot timeout")
	}
	if c.calls != 1 {
		t.Fatal("retried failed reboot")
	}
	assertRestored(t, r, l)
}
func TestPendingRestoreRecoveredWithoutNewReboot(t *testing.T) {
	r, c, l := fixture(t, true)
	l.failRestore = "kitchen"
	if r.Run(context.Background()) == nil {
		t.Fatal("expected restore error")
	}
	if r.Ready() {
		t.Fatal("ready with pending restoration")
	}
	s, _ := r.store.Load()
	if s.Stage != "restoring" {
		t.Fatal(s.Stage)
	}
	l.failRestore = ""
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("recovery resent reboot")
	}
	assertRestored(t, r, l)
}
func TestCrashAfterDurableIntentNeverResends(t *testing.T) {
	r, c, l := fixture(t, true)
	s, err := r.prepare(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for i := range s.Entries {
		e := &s.Entries[i]
		if e.Device.Kind == "wifi_point" {
			e.RestoreNeeded = true
			if err := l.SetNight(context.Background(), e.Device, e.Before.Night.Quiet()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Stage = "reboot_intent"
	s.Submitted = time.Now().Add(-time.Hour)
	if err := r.store.Save(s); err != nil {
		t.Fatal(err)
	}
	if err := r.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 0 {
		t.Fatal("crash recovery sent reboot")
	}
	assertRestored(t, r, l)
}
func TestCorruptStateBlocksNewWork(t *testing.T) {
	r, c, l := fixture(t, true)
	if err := os.WriteFile(filepath.Join(r.store.dir, "operation.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if r.Run(context.Background()) == nil {
		t.Fatal("expected state error")
	}
	if c.calls != 0 || l.writes != 0 {
		t.Fatal("work after corrupt state")
	}
}
func TestStoreExclusiveLock(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenStore(dir)
	if err == nil {
		second.Close()
		t.Fatal("second process acquired lock")
	}
}
func TestStatePermissions(t *testing.T) {
	r, _, _ := fixture(t, false)
	s, err := r.prepare(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.store.Save(s); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(r.store.dir, "operation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}

func TestOriginallyMutedAndNightModePreserved(t *testing.T) {
	r, _, l := fixture(t, true)
	s := l.snapshots["kitchen"]
	s.Volume = &device.Volume{Level: 0.21, Muted: true}
	s.Night = &device.Night{Enabled: true, DND: false, Volume: 0.4, Brightness: 0.2, Windows: []device.Window{{Days: []int{1, 3, 5}, Start: 20, Length: 10}}}
	l.snapshots["kitchen"] = s
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRestored(t, r, l)
	if !l.snapshots["kitchen"].Volume.Muted {
		t.Fatal("originally muted speaker was unmuted")
	}
}

func TestStateDoesNotPersistRawDiagnostics(t *testing.T) {
	r, _, l := fixture(t, false)
	snapshot := l.snapshots["kitchen"]
	snapshot.Raw = map[string]any{"localAuthToken": "secret-lat", "wifi": map[string]any{"ssid": "private-network"}}
	l.snapshots["kitchen"] = snapshot
	s, err := r.prepare(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.store.Save(s); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(r.store.dir, "operation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret-lat") || strings.Contains(string(b), "private-network") {
		t.Fatal("private diagnostics persisted")
	}
}

type fakeDiscovery struct {
	result discovery.Result
	err    error
	forced bool
}

func (d *fakeDiscovery) Discover(_ context.Context, force bool) (discovery.Result, error) {
	d.forced = d.forced || force
	return d.result, d.err
}
func TestIncompleteDiscoveryPreventsAllMutation(t *testing.T) {
	r, c, l := fixture(t, true)
	source := &fakeDiscovery{result: discovery.Result{Devices: r.c.Devices, Complete: false}}
	r.SetDiscovery(source)
	if r.Run(context.Background()) == nil {
		t.Fatal("incomplete discovery accepted")
	}
	if c.calls != 0 || l.writes != 0 || !source.forced {
		t.Fatal("unsafe discovery preflight")
	}
}
func TestDiscoveryReplacesStaticInventory(t *testing.T) {
	r, c, l := fixture(t, true)
	source := &fakeDiscovery{result: discovery.Result{Devices: append([]config.Device(nil), r.c.Devices...), Complete: true}}
	r.c.Devices = nil
	r.SetDiscovery(source)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("did not use discovered inventory")
	}
	assertRestored(t, r, l)
}
func TestAddressRefreshPinsIdentity(t *testing.T) {
	r, _, _ := fixture(t, true)
	state, err := r.prepare(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	state.Entries[1].Device.LocalID = "stable-local-id"
	source := &fakeDiscovery{result: discovery.Result{Devices: []config.Device{{Name: "new-name", CloudName: "Renamed Wifi", Kind: "wifi_point", LocalID: "stable-local-id", IP: "192.0.2.99"}}}}
	r.SetDiscovery(source)
	if err := r.refreshAddresses(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if state.Entries[1].Device.IP != "192.0.2.99" || state.Entries[1].Device.Name != "kitchen" || state.Entries[1].Before.Volume.Level != 0.58 {
		t.Fatal("lost pinned state during address update")
	}
}

func TestQuietSingleAPReboot(t *testing.T) {
	r, c, l := fixture(t, true)
	officeBoot := l.snapshots["office"].Boot
	if err := r.RebootAP(context.Background(), "ap-kitchen"); err != nil {
		t.Fatal(err)
	}
	if c.apCalls != 1 || c.calls != 0 {
		t.Fatalf("ap=%d network=%d", c.apCalls, c.calls)
	}
	s, _ := r.store.Load()
	if len(s.Entries) != 1 || s.Entries[0].Device.Name != "kitchen" || s.APID != "ap-kitchen" {
		t.Fatalf("operation must touch only the kitchen point: %+v", s)
	}
	if !l.snapshots["office"].Boot.Equal(officeBoot) || l.snapshots["office"].Night.Enabled {
		t.Fatal("other point was changed")
	}
	assertRestored(t, r, l)
	if r.RebootAP(context.Background(), "ap-unknown") == nil {
		t.Fatal("unknown AP accepted")
	}
}

func TestMinIntervalPerTarget(t *testing.T) {
	r, c, _ := fixture(t, true)
	r.c.Rebooter.MinInterval = time.Hour
	if err := r.RebootAP(context.Background(), "ap-kitchen"); err != nil {
		t.Fatal(err)
	}
	if err := r.RebootAP(context.Background(), "ap-kitchen"); err == nil || r.Status().LastResult != "too_soon" {
		t.Fatal("repeated AP reboot allowed")
	}
	if err := r.Run(context.Background()); err != nil || c.calls != 1 {
		t.Fatal("a different target must not be blocked", err)
	}
	if err := r.Run(context.Background()); err == nil || c.calls != 1 {
		t.Fatal("repeated network reboot allowed")
	}
}

func TestScheduleSwitch(t *testing.T) {
	r, c, _ := fixture(t, true)
	if err := r.SetSchedule(false); err != nil {
		t.Fatal(err)
	}
	if err := r.Scheduled(context.Background()); err != nil || c.calls != 0 {
		t.Fatal("scheduled run ignored the switch")
	}
	// The switch survives a restart.
	r2 := New(r.c, r.store, c, r.local, true, prometheus.NewRegistry())
	if r2.Status().Schedule {
		t.Fatal("schedule switch not persisted")
	}
	_ = r.SetSchedule(true)
	if err := r.Scheduled(context.Background()); err != nil || c.calls != 1 {
		t.Fatal("scheduled run did not reboot", err)
	}
}

func TestAfterSuccessHookAndStatus(t *testing.T) {
	r, _, _ := fixture(t, true)
	done := make(chan struct{})
	r.AfterSuccess(func(context.Context) { close(done) })
	var seen []Status
	var mu sync.Mutex
	r.OnChange(func(s Status) { mu.Lock(); seen = append(seen, s); mu.Unlock() })
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("after-success hook not called")
	}
	st := r.Status()
	if st.LastResult != "success" || st.LastSuccess.IsZero() || st.LastAttempt.IsZero() || st.Active || st.RestorePending {
		t.Fatalf("%+v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.ContainsFunc(seen, func(s Status) bool { return s.Active }) {
		t.Fatal("active state never reported")
	}
}
