package exporter

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"

	"github.com/prometheus/client_golang/prometheus"
)

type TelemetrySource interface {
	Telemetry(ctx context.Context, groupID string) (cloud.Telemetry, error)
}

// CloudCollector polls the Google Wifi cloud in the background and exposes
// only aggregates: no client names, addresses or identifiers.
type CloudCollector struct {
	source   TelemetrySource
	groupID  string
	interval time.Duration

	mu          sync.RWMutex
	last        cloud.Telemetry
	ok          bool
	lastSuccess time.Time
	errors      float64
}

func NewCloud(source TelemetrySource, groupID string, interval time.Duration) *CloudCollector {
	return &CloudCollector{source: source, groupID: groupID, interval: interval}
}

var (
	apLabels     = []string{"device", "ip"}
	cloudUp      = prometheus.NewDesc("nestwifi_cloud_up", "Last cloud telemetry poll succeeded.", nil, nil)
	cloudSuccess = prometheus.NewDesc("nestwifi_cloud_last_success_timestamp_seconds", "Last successful cloud poll.", nil, nil)
	cloudErrors  = prometheus.NewDesc("nestwifi_cloud_errors_total", "Failed cloud polls since process start.", nil, nil)
	wanOnline    = prometheus.NewDesc("nestwifi_wan_online", "Internet connection status reported by the cloud.", nil, nil)
	traffic      = prometheus.NewDesc("nestwifi_traffic_bits_per_second", "Current whole-network throughput.", []string{"direction"}, nil)
	apOnline     = prometheus.NewDesc("nestwifi_ap_online", "Access point online state reported by the cloud.", append(apLabels, "hardware", "root"), nil)
	apLastSeen   = prometheus.NewDesc("nestwifi_ap_last_seen_timestamp_seconds", "Last time the cloud heard from the access point.", apLabels, nil)
	meshScore    = prometheus.NewDesc("nestwifi_mesh_speed_score", "Mesh link quality to the network (Google numericSpeedRating; OK=50).", apLabels, nil)
	meshRating   = prometheus.NewDesc("nestwifi_mesh_speed_rating", "Mesh link quality label shown in the Google Home app.", append(apLabels, "rating"), nil)
	clients      = prometheus.NewDesc("nestwifi_clients", "Active clients per access point, connection type and band.", []string{"device", "connection", "band"}, nil)
	stationsDesc = prometheus.NewDesc("nestwifi_stations", "Client devices known to the network.", []string{"state"}, nil)
)

func (c *CloudCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{cloudUp, cloudSuccess, cloudErrors, wanOnline, traffic, apOnline, apLastSeen, meshScore, meshRating, clients, stationsDesc} {
		ch <- d
	}
}

func (c *CloudCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	metric := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	metric(cloudUp, boolFloat(c.ok))
	ch <- prometheus.MustNewConstMetric(cloudErrors, prometheus.CounterValue, c.errors)
	if c.lastSuccess.IsZero() {
		return
	}
	// The last good values are kept while the cloud is briefly unreachable;
	// alert on cloud_up or last_success rather than on missing series.
	t := c.last
	metric(cloudSuccess, float64(c.lastSuccess.Unix()))
	metric(wanOnline, boolFloat(t.WANOnline))
	metric(traffic, t.TxBps, "transmit")
	metric(traffic, t.RxBps, "receive")
	metric(stationsDesc, float64(t.Connected), "connected")
	metric(stationsDesc, float64(t.Known-t.Connected), "disconnected")
	names := map[string]string{}
	for _, ap := range t.Group.AccessPoints {
		p := ap.Properties
		name := discovery.Slug(ap.Name())
		names[ap.ID] = name
		root := "false"
		if p.Root {
			root = "true"
		}
		metric(apOnline, boolFloat(t.APOnline[ap.ID]), name, p.IP, p.Hardware, root)
		if !p.LastSeen.IsZero() {
			metric(apLastSeen, float64(p.LastSeen.Unix()), name, p.IP)
		}
		if m, ok := t.Mesh[ap.ID]; ok {
			metric(meshScore, m.Score, name, p.IP)
			metric(meshRating, 1, name, p.IP, m.Rating)
		}
	}
	for k, n := range t.Clients {
		device, ok := names[k.APID]
		if !ok {
			device = "unknown"
		}
		metric(clients, float64(n), device, k.Connection, k.Band)
	}
}

// Last returns the last successful telemetry and whether the latest poll succeeded.
func (c *CloudCollector) Last() (t cloud.Telemetry, everOK, upNow bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.last, !c.lastSuccess.IsZero(), c.ok
}

func (c *CloudCollector) poll(ctx context.Context) {
	task, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t, err := c.source.Telemetry(task, c.groupID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ok = err == nil
	if err != nil {
		c.errors++
		slog.Warn("cloud telemetry unavailable", "error", err)
		return
	}
	c.last, c.lastSuccess = t, time.Now()
}

// Run polls every interval; after errors (notably HTTP 429 quota exhaustion)
// it backs off exponentially up to 30 minutes.
func (c *CloudCollector) Run(ctx context.Context) {
	wait := c.interval
	for {
		c.poll(ctx)
		if _, _, ok := c.Last(); ok {
			wait = c.interval
		} else {
			wait = min(2*wait, 30*time.Minute)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
