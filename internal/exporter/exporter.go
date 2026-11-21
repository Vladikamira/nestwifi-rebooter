package exporter

import (
	"context"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"

	"github.com/prometheus/client_golang/prometheus"
)

type Inspector interface {
	Inspect(context.Context, config.Device, bool) (device.Snapshot, error)
}
type record struct {
	Snapshot    device.Snapshot
	OK          bool
	Duration    float64
	LastSuccess time.Time
	Errors      float64
}
type Collector struct {
	mu              sync.RWMutex
	config          config.Config
	client          Inspector
	records         map[string]record
	initialized     bool
	desc            map[string]*prometheus.Desc
	devices         []config.Device
	discovery       discovery.Source
	discovered      discovery.Result
	discoveryErrors float64
}

func New(c config.Config, client Inspector) *Collector {
	x := &Collector{
		config:  c,
		client:  client,
		records: map[string]record{},
		desc:    map[string]*prometheus.Desc{},
		devices: slices.Clone(c.Devices),
	}
	for name, help := range map[string]string{
		"device_up":                      "Last local API collection succeeded.",
		"cast_up":                        "Last optional Cast volume collection succeeded.",
		"scrape_duration_seconds":        "Duration of the last background collection.",
		"last_success_timestamp_seconds": "Last successful local collection.",
		"scrape_errors_total":            "Failed local collections since process start.",
		"uptime_seconds":                 "Uptime reported by the device.",
		"boot_timestamp_seconds":         "Estimated boot timestamp (collection time minus uptime).",
		"cast_volume_ratio":              "Cast media volume, not a guarantee of silence.",
		"cast_muted":                     "Cast media muted flag.",
		"backhaul_wired":                 "1 if the mesh uplink is Ethernet, 0 if wireless; absent for the root router.",
	} {
		x.desc[name] = prometheus.NewDesc("nestwifi_"+name, help, []string{"device", "ip", "kind"}, nil)
	}
	x.desc["info"] = prometheus.NewDesc("nestwifi_device_info", "Model, firmware, router role and update status.",
		[]string{"device", "ip", "kind", "model", "firmware", "role", "update_status"}, nil)
	x.desc["value"] = prometheus.NewDesc("nestwifi_device_value",
		"Allowlisted numeric diagnostics; booleans are 0/1. No strings or identifiers.",
		[]string{"device", "ip", "kind", "path"}, nil)
	for _, name := range []string{"complete", "expected_devices", "matched_devices", "last_scan_timestamp_seconds", "errors_total"} {
		x.desc["discovery_"+name] = prometheus.NewDesc("nestwifi_discovery_"+name,
			"Discovery status; complete means every HomeGraph Wifi device was identified on the LAN.", nil, nil)
	}
	return x
}
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.desc {
		ch <- d
	}
}
func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.discovery != nil {
		d := c.discovered
		gauge := func(name string, v float64) {
			ch <- prometheus.MustNewConstMetric(c.desc["discovery_"+name], prometheus.GaugeValue, v)
		}
		gauge("complete", boolFloat(d.Complete))
		gauge("expected_devices", float64(d.Expected))
		gauge("matched_devices", float64(d.Matched))
		if !d.At.IsZero() {
			gauge("last_scan_timestamp_seconds", float64(d.At.Unix()))
		}
		ch <- prometheus.MustNewConstMetric(c.desc["discovery_errors_total"], prometheus.CounterValue, c.discoveryErrors)
	}

	for _, d := range c.devices {
		r := c.records[d.Key()]
		labels := []string{d.Name, d.IP, d.Kind}
		emit := func(name string, value float64) {
			kind := prometheus.GaugeValue
			if name == "scrape_errors_total" {
				kind = prometheus.CounterValue
			}
			ch <- prometheus.MustNewConstMetric(c.desc[name], kind, value, labels...)
		}
		emit("device_up", boolFloat(r.OK))
		if d.Kind == "wifi_point" && c.config.Exporter.Cast {
			emit("cast_up", boolFloat(r.OK && r.Snapshot.Volume != nil))
		}
		emit("scrape_duration_seconds", r.Duration)
		emit("scrape_errors_total", r.Errors)
		if !r.LastSuccess.IsZero() {
			emit("last_success_timestamp_seconds", float64(r.LastSuccess.Unix()))
		}
		if !r.OK {
			continue
		}
		s := r.Snapshot
		emit("uptime_seconds", s.Uptime)
		emit("boot_timestamp_seconds", float64(s.Boot.Unix()))
		ch <- prometheus.MustNewConstMetric(c.desc["info"], prometheus.GaugeValue, 1, append(labels, s.Model, s.Firmware, s.Role, s.Update)...)
		if s.Backhaul != "" {
			emit("backhaul_wired", boolFloat(s.Backhaul == "wired"))
		}
		if s.Volume != nil {
			emit("cast_volume_ratio", s.Volume.Level)
			emit("cast_muted", boolFloat(s.Volume.Muted))
		}
		for path, value := range NumericValues(s.Raw) {
			ch <- prometheus.MustNewConstMetric(c.desc["value"], prometheus.GaugeValue, value, append(labels, path)...)
		}
	}
}

// NumericValues deliberately excludes addresses, device IDs, names, SSIDs,
// certificates and token hashes. No arbitrary JSON keys are used as labels.
func NumericValues(raw map[string]any) map[string]float64 {
	out := map[string]float64{}
	allowed := map[string][]string{
		"system":            {"uptime", "lan0Link", "ledIntensity"},
		"software":          {"blockingUpdate", "updateProgress", "updateRequired"},
		"wan":               {"online", "ethernetLink", "captivePortal", "invalidCredentials"},
		"device_info":       {"uptime"},
		"net":               {"online", "ethernet_connected"},
		"wifi":              {"signal_level", "noise_level", "wpa_state", "wpa_configured", "has_changes"},
		"night_mode_params": {"enabled", "do_not_disturb", "led_brightness", "volume"},
		"audio":             {"digital"},
		"setup":             {"setup_state", "tos_accepted"},
	}
	for section, keys := range allowed {
		m, _ := raw[section].(map[string]any)
		for _, k := range keys {
			v := m[k]
			var value float64
			switch x := v.(type) {
			case bool:
				value = boolFloat(x)
			case float64:
				value = x
			default:
				continue
			}
			if !math.IsNaN(value) && !math.IsInf(value, 0) {
				out[strings.Join([]string{section, k}, ".")] = value
			}
		}
	}
	return out
}

func (c *Collector) Ready() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.initialized }

// LocalState is a device with its last local collection result.
type LocalState struct {
	Device   config.Device
	OK       bool
	Snapshot device.Snapshot
}

// States returns a consistent copy for other publishers (e.g. MQTT).
func (c *Collector) States() ([]LocalState, discovery.Result) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]LocalState, 0, len(c.devices))
	for _, d := range c.devices {
		r := c.records[d.Key()]
		out = append(out, LocalState{Device: d, OK: r.OK, Snapshot: r.Snapshot})
	}
	return out, c.discovered
}

func (c *Collector) SetDiscovery(source discovery.Source) {
	c.discovery = source
}

func (c *Collector) collect(ctx context.Context) {
	if c.discovery != nil {
		result, err := c.discovery.Discover(ctx, false)
		c.mu.Lock()
		c.discovered = result
		if err != nil {
			c.discoveryErrors++
		}
		// Update by stable identity (IP/name may change). Devices that vanish and
		// HomeGraph devices never found locally are kept, so they report
		// device_up=0 instead of disappearing.
		same := func(a, b config.Device) bool {
			return a.Key() == b.Key() || a.CloudID != "" && a.CloudID == b.CloudID
		}
		for _, found := range result.Devices {
			// Replace every entry for this device, including a placeholder
			// (ip="") added while it was missing, so no stale series remains.
			c.devices = slices.DeleteFunc(c.devices, func(d config.Device) bool {
				if !same(d, found) {
					return false
				}
				if d.Key() != found.Key() {
					delete(c.records, d.Key())
				}
				return true
			})
			if len(c.devices) < 1024 {
				c.devices = append(c.devices, found)
			}
		}
		for _, missing := range result.Missing {
			if !slices.ContainsFunc(c.devices, func(d config.Device) bool { return same(d, missing) }) && len(c.devices) < 1024 {
				c.devices = append(c.devices, missing)
			}
		}
		c.mu.Unlock()
	}
	c.mu.RLock()
	devices := append([]config.Device(nil), c.devices...)
	c.mu.RUnlock()
	sem := make(chan struct{}, c.config.Exporter.Concurrency)
	var wg sync.WaitGroup
	for _, d := range devices {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(d config.Device) {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			task, cancel := context.WithTimeout(ctx, c.config.Exporter.Timeout)
			defer cancel()
			s, err := c.client.Inspect(task, d, c.config.Exporter.Cast && d.Kind == "wifi_point")
			c.mu.Lock()
			r := c.records[d.Key()]
			r.OK = err == nil
			r.Duration = time.Since(start).Seconds()
			r.Snapshot = s
			if err == nil {
				r.LastSuccess = time.Now()
			} else {
				r.Errors++
			}
			c.records[d.Key()] = r
			c.mu.Unlock()
		}(d)
	}
	wg.Wait()
	c.mu.Lock()
	c.initialized = true
	c.mu.Unlock()
}

func (c *Collector) Run(ctx context.Context) {
	c.collect(ctx)
	ticker := time.NewTicker(c.config.Exporter.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(ctx)
		}
	}
}
