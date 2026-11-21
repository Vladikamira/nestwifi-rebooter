package rebooter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/prometheus/client_golang/prometheus"
)

type Cloud interface {
	CheckGroup(ctx context.Context, groupID string, ips []string) error
	AccessPointIP(ctx context.Context, groupID, apID string) (string, error)
	Reboot(ctx context.Context, groupID string) error
	RebootAP(ctx context.Context, apID string) error
	Refresh(context.Context, config.Device) error
}
type Local interface {
	Inspect(context.Context, config.Device, bool) (device.Snapshot, error)
	SetNight(context.Context, config.Device, device.Night) error
	RestoreVolume(context.Context, config.Device, device.Volume) error
}
type Runner struct {
	mu                                        sync.Mutex
	c                                         config.Config
	store                                     *Store
	cloud                                     Cloud
	local                                     Local
	enabled                                   bool
	ready                                     atomic.Bool
	pending, active, lastSuccess, lastAttempt prometheus.Gauge
	jobs                                      *prometheus.CounterVec
	discovery                                 discovery.Source

	statusMu sync.Mutex
	status   Status
	onChange func(Status)
	// afterSuccess runs (in the background) after a confirmed network reboot,
	// e.g. a speedtest.
	afterSuccess func(context.Context)
}

// Status is what Home Assistant shows about the rebooter.
type Status struct {
	Active         bool      `json:"active"`
	RestorePending bool      `json:"restore_pending"`
	Enabled        bool      `json:"enabled"`
	Schedule       bool      `json:"schedule"`
	LastResult     string    `json:"last_result,omitempty"` // success, failed, dry_run, too_soon, …
	LastResultAt   time.Time `json:"last_result_at,omitzero"`
	LastError      string    `json:"last_error,omitempty"` // why the last job did not succeed
	LastAttempt    time.Time `json:"last_attempt,omitzero"`
	LastSuccess    time.Time `json:"last_success,omitzero"`
	NextRun        time.Time `json:"next_run,omitzero"`
}

func (r *Runner) update(f func(*Status)) {
	r.statusMu.Lock()
	f(&r.status)
	s, notify := r.status, r.onChange
	r.statusMu.Unlock()
	if notify != nil {
		notify(s)
	}
}

func (r *Runner) Status() Status { r.statusMu.Lock(); defer r.statusMu.Unlock(); return r.status }

// OnChange registers a callback for status changes (one subscriber).
func (r *Runner) OnChange(f func(Status)) { r.statusMu.Lock(); r.onChange = f; r.statusMu.Unlock() }

// AfterSuccess registers work to run after a confirmed network reboot.
func (r *Runner) AfterSuccess(f func(context.Context)) { r.afterSuccess = f }

// errPreflight marks failures before anything was changed, so retrying is safe.
var errPreflight = errors.New("preflight failed")

func (r *Runner) result(outcome string, err error) {
	r.jobs.WithLabelValues(outcome).Inc()
	msg := ""
	if err != nil {
		if msg = err.Error(); len(msg) > 250 {
			msg = msg[:250] + "…" // HA state values are limited to 255 characters
		}
		slog.Warn("job did not succeed", "outcome", outcome, "error", msg)
	}
	r.update(func(s *Status) { s.LastResult, s.LastResultAt, s.LastError = outcome, time.Now(), msg })
}

func (r *Runner) setActive(on bool) {
	r.active.Set(boolGauge(on))
	r.update(func(s *Status) { s.Active = on })
}

func (r *Runner) setPending(on bool) {
	r.pending.Set(boolGauge(on))
	r.update(func(s *Status) { s.RestorePending = on })
}

func boolGauge(on bool) float64 {
	if on {
		return 1
	}
	return 0
}

// SetSchedule enables or disables scheduled runs; persisted across restarts.
func (r *Runner) SetSchedule(on bool) error {
	if err := r.store.SetScheduleEnabled(on); err != nil {
		return err
	}
	r.update(func(s *Status) { s.Schedule = on })
	return nil
}

// SetNextRun records the next cron time for display.
func (r *Runner) SetNextRun(t time.Time) { r.update(func(s *Status) { s.NextRun = t }) }

// Scheduled is the cron entry point: it honours the schedule switch and
// retries failed preflights (nothing was changed yet), e.g. after a transient
// Google cloud error, so one hiccup does not skip the reboot for a whole day.
func (r *Runner) Scheduled(ctx context.Context) error {
	if !r.Status().Schedule {
		slog.Info("scheduled reboot skipped: schedule switched off")
		return nil
	}
	retries, wait := r.c.Rebooter.PreflightRetries, r.c.Rebooter.PreflightRetryInterval
	for attempt := 0; ; attempt++ {
		err := r.Run(ctx)
		if err == nil || !errors.Is(err, errPreflight) || attempt >= retries {
			return err
		}
		slog.Info("scheduled reboot preflight failed; will retry", "attempt", attempt+1, "of", retries+1, "in", wait)
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func New(c config.Config, store *Store, cloud Cloud, local Local, enabled bool, registry *prometheus.Registry) *Runner {
	r := &Runner{c: c, store: store, cloud: cloud, local: local, enabled: enabled}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: "nestwifi_rebooter_" + name, Help: help})
	}
	r.pending = gauge("restore_pending", "An unfinished operation still requires recovery.")
	r.active = gauge("active", "A job or recovery is running.")
	r.lastSuccess = gauge("last_success_timestamp_seconds", "Last fully confirmed reboot with settings restored.")
	r.lastAttempt = gauge("last_attempt_timestamp_seconds", "Last cloud reboot intent (not proof of acceptance).")
	r.jobs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nestwifi_rebooter_jobs_total", Help: "Jobs by outcome since process start."}, []string{"outcome"})
	enabledGauge := gauge("enabled", "Explicit reboot enablement (recovery remains enabled).")
	if enabled {
		enabledGauge.Set(1)
	}
	registry.MustRegister(r.pending, r.active, r.lastSuccess, r.lastAttempt, r.jobs, enabledGauge)
	r.status.Enabled = enabled
	r.status.Schedule = store.ScheduleEnabled()
	if s, err := store.Load(); err == nil && s != nil {
		r.status.LastAttempt = s.Submitted
		if s.Stage == "done" && s.Outcome == "success" {
			r.status.LastSuccess = s.Updated
		}
		if s.Stage == "done" {
			r.status.LastResult, r.status.LastResultAt = s.Outcome, s.Updated
		}
	}
	return r
}
func (r *Runner) Ready() bool                          { return r.ready.Load() }
func (r *Runner) SetDiscovery(source discovery.Source) { r.discovery = source }
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (r *Runner) save(s *State) error {
	if err := r.store.Save(s); err != nil {
		r.ready.Store(false)
		r.setPending(true)
		return fmt.Errorf("cannot persist operation: %w", err)
	}
	r.setPending(s.Stage != "done")
	return nil
}

// prepare verifies the whole inventory even for a single-AP reboot, then
// snapshots the devices the operation touches.
func (r *Runner) prepare(ctx context.Context, apID string) (*State, error) {
	if r.c.Rebooter.GroupID == "" {
		return nil, errors.New("rebooter.group_id is required")
	}
	devices := r.c.Devices
	if r.discovery != nil {
		result, err := r.discovery.Discover(ctx, true)
		if err != nil || !result.Complete {
			reason := strings.Join(result.Problems, "; ")
			if reason == "" && err != nil {
				reason = err.Error()
			}
			return nil, fmt.Errorf("inventory incomplete (%d/%d matched): %s; reboot prohibited", result.Matched, result.Expected, reason)
		}
		devices = result.Devices
	}
	if len(devices) == 0 {
		return nil, errors.New("empty inventory; reboot prohibited")
	}
	ips := make([]string, len(devices))
	for i, d := range devices {
		ips[i] = d.IP
	}
	if err := r.cloud.CheckGroup(ctx, r.c.Rebooter.GroupID, ips); err != nil {
		return nil, err
	}
	if apID != "" {
		ip, err := r.cloud.AccessPointIP(ctx, r.c.Rebooter.GroupID, apID)
		if err != nil {
			return nil, err
		}
		devices = slices.DeleteFunc(slices.Clone(devices), func(d config.Device) bool { return d.IP != ip })
		if len(devices) != 1 {
			return nil, errors.New("access point not uniquely found on the LAN")
		}
	}
	s := &State{Version: 1, Created: time.Now(), Stage: "quieting", Outcome: "interrupted", GroupID: r.c.Rebooter.GroupID, APID: apID}
	for _, d := range devices {
		task, cancel := context.WithTimeout(ctx, 40*time.Second)
		snapshot, err := r.local.Inspect(task, d, d.Kind == "wifi_point")
		cancel()
		if err != nil {
			return nil, fmt.Errorf("preflight %s: %w", d.Name, err)
		}
		if d.Kind == "wifi_point" && (snapshot.Night == nil || snapshot.Volume == nil) {
			return nil, fmt.Errorf("preflight %s missing night/volume snapshot", d.Name)
		}
		s.Entries = append(s.Entries, Entry{Device: d, Before: snapshot})
	}
	return s, nil
}

func (r *Runner) waitAll(ctx context.Context, s *State) error {
	deadline := s.Submitted.Add(r.c.Rebooter.WaitTimeout)
	for time.Now().Before(deadline) {
		if r.discovery != nil {
			task, cancel := context.WithDeadline(ctx, deadline)
			err := r.refreshAddresses(task, s)
			cancel()
			if err != nil {
				return err
			}
		}
		all := true
		for _, entry := range s.Entries {
			task, cancel := context.WithDeadline(ctx, deadline)
			probe, stop := context.WithTimeout(task, 15*time.Second)
			after, err := r.local.Inspect(probe, entry.Device, false)
			stop()
			cancel()
			if err != nil || !after.Boot.After(entry.Before.Boot.Add(3*time.Second)) {
				all = false
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if all {
			return sleep(ctx, r.c.Rebooter.Settle)
		}
		if err := sleep(ctx, r.c.Rebooter.PollInterval); err != nil {
			return err
		}
	}
	return errors.New("timeout confirming a new boot on every configured device")
}

// Only endpoint addresses/names may change during recovery. Membership and
// saved settings remain pinned to the original operation's stable identities.
func (r *Runner) refreshAddresses(ctx context.Context, s *State) error {
	if r.discovery == nil {
		return nil
	}
	result, _ := r.discovery.Discover(ctx, false)
	changed := false
	for i := range s.Entries {
		e := &s.Entries[i]
		var matches []config.Device
		for _, d := range result.Devices {
			if d.Key() == e.Device.Key() {
				matches = append(matches, d)
			}
		}
		if len(matches) == 1 && (e.Device.IP != matches[0].IP || e.Device.CloudName != matches[0].CloudName) {
			e.Device.IP = matches[0].IP
			e.Device.CloudName = matches[0].CloudName
			changed = true
		}
	}
	if changed {
		return r.save(s)
	}
	return nil
}

func (r *Runner) restore(ctx context.Context, s *State) error {
	if err := r.refreshAddresses(ctx, s); err != nil {
		return err
	}
	s.Stage = "restoring"
	if err := r.save(s); err != nil {
		return err
	}
	var failures []error
	for i := range s.Entries {
		e := &s.Entries[i]
		if !e.RestoreNeeded {
			continue
		}
		task, cancel := context.WithTimeout(ctx, 60*time.Second)
		// Always refresh after boot, even if the cached LAT hasn't expired.
		err := r.cloud.Refresh(task, e.Device)
		if err == nil {
			_, err = r.local.Inspect(task, e.Device, false) // Revalidate identity before writes.
		}
		if err == nil {
			err = r.local.SetNight(task, e.Device, *e.Before.Night)
		}
		if err == nil {
			err = r.local.RestoreVolume(task, e.Device, *e.Before.Volume)
		}
		cancel()
		if err != nil {
			slog.Warn("settings restoration pending", "device", e.Device.Name, "error", err)
			failures = append(failures, fmt.Errorf("restore %s failed", e.Device.Name))
			continue
		}
		e.RestoreNeeded = false
		if err := r.save(s); err != nil {
			return err
		}
		slog.Info("original settings restored", "device", e.Device.Name)
	}
	if len(failures) > 0 {
		r.ready.Store(false)
		return errors.Join(failures...)
	}
	s.Stage = "done"
	if err := r.save(s); err != nil {
		return err
	}
	r.ready.Store(true)
	if s.Outcome == "success" {
		now := time.Now()
		r.lastSuccess.Set(float64(now.Unix()))
		r.update(func(st *Status) { st.LastSuccess = now })
	}
	return nil
}

// recoverLocked never issues reboot, including when a crash occurred between
// persisting reboot_intent and sending the HTTP request. At-most-once is safer.
func (r *Runner) recoverLocked(ctx context.Context, s *State) error {
	r.setPending(true)
	r.ready.Store(false)
	if (s.Stage == "reboot_intent" || s.Stage == "waiting") && !s.Submitted.IsZero() {
		if err := r.waitAll(ctx, s); err == nil {
			s.Outcome = "success"
		} else {
			s.Outcome = "failed"
			slog.Warn("recovery boot confirmation incomplete", "error", err)
		}
	}
	return r.restore(ctx, s)
}
func (r *Runner) Recover(ctx context.Context) error {
	if !r.mu.TryLock() {
		return nil
	}
	defer r.mu.Unlock()
	s, err := r.store.Load()
	if err != nil {
		r.ready.Store(false)
		r.setPending(true)
		return err
	}
	if s == nil || s.Stage == "done" {
		r.ready.Store(true)
		r.setPending(false)
		return nil
	}
	r.setActive(true)
	defer r.setActive(false)
	slog.Warn("recovering unfinished operation; reboot will NOT be resent", "stage", s.Stage)
	return r.recoverLocked(ctx, s)
}

// Run quietly reboots the whole mesh network.
func (r *Runner) Run(ctx context.Context) error { return r.run(ctx, "") }

// RebootAP quietly reboots one access point (cloud AP ID) with the same safeguards.
func (r *Runner) RebootAP(ctx context.Context, apID string) error {
	if apID == "" {
		return errors.New("access point ID required")
	}
	return r.run(ctx, apID)
}

func (r *Runner) run(ctx context.Context, apID string) error {
	if !r.mu.TryLock() {
		err := errors.New("another operation is running")
		r.result("skipped_busy", err)
		return err
	}
	defer r.mu.Unlock()
	r.setActive(true)
	defer r.setActive(false)
	previous, err := r.store.Load()
	if err != nil {
		r.ready.Store(false)
		r.setPending(true)
		return err
	}
	if previous != nil && previous.Stage != "done" {
		return r.recoverLocked(ctx, previous)
	}
	if previous != nil && previous.APID == apID && time.Since(previous.Submitted) < r.c.Rebooter.MinInterval {
		err := fmt.Errorf("last reboot of this target was %s ago; minimum interval is %s",
			time.Since(previous.Submitted).Round(time.Minute), r.c.Rebooter.MinInterval)
		r.result("too_soon", err)
		return err
	}
	s, err := r.prepare(ctx, apID)
	if err != nil {
		r.result("preflight_failed", err)
		return fmt.Errorf("%w: %w", errPreflight, err)
	}
	if !r.enabled {
		r.result("dry_run", nil)
		slog.Info("dry run: preflight passed; no settings changed and no reboot sent")
		return nil
	}
	if err := r.save(s); err != nil {
		return err
	}
	r.ready.Store(false)
	for i := range s.Entries {
		e := &s.Entries[i]
		if e.Device.Kind != "wifi_point" {
			continue
		}
		// Persist before mutating, including when the POST response gets lost.
		e.RestoreNeeded = true
		if err := r.save(s); err != nil {
			return err
		}
		task, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = r.local.SetNight(task, e.Device, e.Before.Night.Quiet())
		cancel()
		if err != nil {
			s.Outcome = "failed"
			err = fmt.Errorf("quiet %s failed; reboot NOT sent: %w", e.Device.Name, err)
			r.result("quiet_failed", err)
			return errors.Join(err, r.restore(ctx, s))
		}
	}
	// Verify every Point again after all mutations, before the irreversible command.
	if err := sleep(ctx, r.c.Rebooter.QuietSettle); err != nil {
		s.Outcome = "failed"
		return errors.Join(err, r.restore(ctx, s))
	}
	for _, e := range s.Entries {
		if e.Device.Kind != "wifi_point" {
			continue
		}
		task, cancel := context.WithTimeout(ctx, 20*time.Second)
		snapshot, e2 := r.local.Inspect(task, e.Device, true)
		cancel()
		quiet := e2 == nil && snapshot.Night != nil && device.NightEqual(*snapshot.Night, e.Before.Night.Quiet()) &&
			snapshot.Volume != nil && snapshot.Volume.Level <= 0.001
		if !quiet {
			s.Outcome = "failed"
			err := fmt.Errorf("quiet mode of %s not confirmed; reboot NOT sent", e.Device.Name)
			r.result("quiet_failed", err)
			return errors.Join(err, r.restore(ctx, s))
		}
	}
	s.Stage = "reboot_intent"
	s.Submitted = time.Now()
	if err := r.save(s); err != nil {
		return err
	}
	r.lastAttempt.Set(float64(s.Submitted.Unix()))
	r.update(func(st *Status) { st.LastAttempt = s.Submitted })
	task, cancel := context.WithTimeout(ctx, 40*time.Second)
	var rebootErr error
	if apID == "" {
		slog.Warn("sending ONE cloud reboot for entire mesh", "group_id", s.GroupID)
		rebootErr = r.cloud.Reboot(task, s.GroupID)
	} else {
		slog.Warn("sending ONE cloud reboot for access point", "device", s.Entries[0].Device.Name)
		rebootErr = r.cloud.RebootAP(task, apID)
	}
	cancel()
	if rebootErr != nil {
		slog.Warn("cloud reboot result uncertain; will not retry", "error", rebootErr)
	}
	s.Stage = "waiting"
	if err := r.save(s); err != nil {
		return err
	}
	bootErr := r.waitAll(ctx, s)
	if bootErr == nil {
		s.Outcome = "success"
	} else {
		s.Outcome = "failed"
		slog.Warn("boot confirmation incomplete", "error", bootErr)
	}
	restoreErr := r.restore(ctx, s)
	if err := errors.Join(bootErr, restoreErr); err != nil {
		r.result("failed", err)
		return err
	}
	r.result("success", nil)
	slog.Info("reboot confirmed; all original settings restored", "access_point", apID)
	if apID == "" && r.afterSuccess != nil {
		go r.afterSuccess(ctx)
	}
	return nil
}
