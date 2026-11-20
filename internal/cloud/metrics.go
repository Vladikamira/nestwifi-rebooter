package cloud

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterMetrics never performs network I/O or exposes credentials.
func (m *Manager) RegisterMetrics(registry *prometheus.Registry) {
	gauge := func(name, help string, pick func(access, graph time.Time) time.Time) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
			if t := pick(m.RefreshTimes()); !t.IsZero() {
				return float64(t.Unix())
			}
			return 0
		})
	}
	registry.MustRegister(
		gauge("nestwifi_auth_access_last_refresh_timestamp_seconds", "Last successful master-to-access token exchange; 0 if unused.",
			func(a, _ time.Time) time.Time { return a }),
		gauge("nestwifi_auth_homegraph_last_refresh_timestamp_seconds", "Last successful HomeGraph token retrieval; 0 if unused.",
			func(_, g time.Time) time.Time { return g }),
	)
}
