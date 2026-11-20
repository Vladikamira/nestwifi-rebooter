package exporter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/prometheus/client_golang/prometheus"
)

func TestNumericAllowlistExcludesSecretsAndDynamicKeys(t *testing.T) {
	raw := map[string]any{"localAuthToken": "secret", "wifi": map[string]any{"signal_level": float64(-45), "ssid": "private", "192.168.1.99": float64(42)}, "device_info": map[string]any{"uptime": float64(123), "mac_address": "private"}, "night_mode_params": map[string]any{"enabled": true}}
	values := NumericValues(raw)
	if len(values) != 3 || values["wifi.signal_level"] != -45 || values["night_mode_params.enabled"] != 1 {
		t.Fatal(values)
	}
}

type inspector struct{}

func (inspector) Inspect(_ context.Context, d config.Device, _ bool) (device.Snapshot, error) {
	if d.Name == "offline" {
		return device.Snapshot{}, errors.New("offline")
	}
	return device.Snapshot{Uptime: 123, Boot: time.Now().Add(-123 * time.Second), Raw: map[string]any{}}, nil
}
func TestPartialFailureAndMetrics(t *testing.T) {
	var cfg config.Config
	cfg.Devices = []config.Device{{Name: "online", IP: "192.0.2.1", Kind: "router"}, {Name: "offline", IP: "192.0.2.2", Kind: "router"}}
	cfg.Exporter.Concurrency = 2
	cfg.Exporter.Timeout = time.Second
	c := New(cfg, inspector{})
	c.collect(context.Background())
	r := prometheus.NewRegistry()
	r.MustRegister(c)
	metrics, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if !c.Ready() {
		t.Fatal("not initialized")
	}
	foundUp := false
	for _, family := range metrics {
		if strings.Contains(family.GetName(), "device_up") {
			foundUp = true
			if len(family.Metric) != 2 {
				t.Fatal("missing offline metric")
			}
		}
	}
	if !foundUp {
		t.Fatal("no device_up metric")
	}
	if !c.records[cfg.Devices[0].Key()].OK || c.records[cfg.Devices[1].Key()].OK {
		t.Fatal("partial failure propagated")
	}
}

type dynamicSource struct{ devices []config.Device }

func (d *dynamicSource) Discover(context.Context, bool) (discovery.Result, error) {
	return discovery.Result{Devices: d.devices, At: time.Now()}, nil
}

type dynamicInspector struct{ online bool }

func (d *dynamicInspector) Inspect(context.Context, config.Device, bool) (device.Snapshot, error) {
	if !d.online {
		return device.Snapshot{}, errors.New("offline")
	}
	return device.Snapshot{Uptime: 12}, nil
}
func TestDiscoveryRetainsMissingDevicesForDownMetrics(t *testing.T) {
	var cfg config.Config
	cfg.Exporter.Concurrency = 1
	cfg.Exporter.Timeout = time.Second
	client := &dynamicInspector{online: true}
	source := &dynamicSource{devices: []config.Device{{Name: "found", IP: "192.168.86.32", Kind: "wifi_point", LocalID: "stable-id"}}}
	c := New(cfg, client)
	c.SetDiscovery(source)
	c.collect(context.Background())
	source.devices = nil
	client.online = false
	c.collect(context.Background())
	key := c.devices[0].Key()
	if len(c.devices) != 1 || c.records[key].OK {
		t.Fatal("missing device disappeared instead of going down")
	}
	source.devices = []config.Device{{Name: "kitchen-wifi", IP: "192.168.86.33", Kind: "wifi_point", LocalID: "stable-id", CloudID: "g"}}
	client.online = true
	c.collect(context.Background())
	if len(c.devices) != 1 || c.devices[0].Name != "kitchen-wifi" || c.devices[0].IP != "192.168.86.33" || !c.records[key].OK {
		t.Fatal("IP/name change duplicated stable device")
	}
}
