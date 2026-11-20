package exporter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha/mqtttest"
)

type localFixture struct{}

func (localFixture) Inspect(_ context.Context, d config.Device, _ bool) (device.Snapshot, error) {
	return device.Snapshot{Uptime: 60, Boot: time.Date(2026, 11, 20, 3, 0, 0, 0, time.UTC), Model: "MISTRAL",
		Firmware: "14150", Role: "root", Update: "idle"}, nil
}

func TestPublisherHomeAssistantDiscovery(t *testing.T) {
	b := mqtttest.Start(t)
	m, err := mqttha.Connect(config.MQTT{URL: b.URL, TopicPrefix: "nestwifi", DiscoveryPrefix: "homeassistant"}, "exporter")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	var cfg config.Config
	cfg.Exporter.Concurrency, cfg.Exporter.Timeout = 1, time.Second
	// Only the root is reachable locally; the Point exists only in the cloud.
	cfg.Devices = []config.Device{{Name: "living-room-wifi", IP: "192.0.2.1", Kind: "router"}}
	local := New(cfg, localFixture{})
	local.collect(context.Background())
	cloudC := NewCloud(&fakeTelemetry{t: telemetryFixture()}, "", 0)
	cloudC.poll(context.Background())

	p := NewPublisher(m, local, cloudC, time.Minute)
	mqtttest.Wait(t, func() bool { return b.Last("nestwifi/network/state") != "" })
	p.publish()

	root := b.Last("homeassistant/device/nestwifi_ap_r/config")
	point := b.Last("homeassistant/device/nestwifi_ap_p/config")
	var d struct {
		Dev   map[string]any            `json:"dev"`
		Cmps  map[string]map[string]any `json:"cmps"`
		State string                    `json:"state_topic"`
	}
	if err := json.Unmarshal([]byte(point), &d); err != nil {
		t.Fatal("point discovery missing", err)
	}
	if d.Dev["name"] != "Kitchen Wifi" || d.Dev["via_device"] != "nestwifi_network_g" || d.State != "nestwifi/ap/kitchen-wifi/state" {
		t.Fatalf("%+v", d)
	}
	if _, ok := d.Cmps["mesh"]; !ok {
		t.Fatal("point has mesh quality")
	}
	if _, ok := d.Cmps["boot"]; ok {
		t.Fatal("boot time unknown for an unreachable device must not be declared")
	}
	if !strings.Contains(root, `"boot"`) || strings.Contains(root, `"mesh"`) {
		t.Fatal("root: boot expected, mesh not expected:", root)
	}

	var s apState
	if err := json.Unmarshal([]byte(b.Last("nestwifi/ap/kitchen-wifi/state")), &s); err != nil {
		t.Fatal(err)
	}
	if !s.Online || s.Reachable || s.Clients5 != 3 || s.MeshRating != "OK" {
		t.Fatalf("%+v", s)
	}
	var rs apState
	_ = json.Unmarshal([]byte(b.Last("nestwifi/ap/living-room-wifi/state")), &rs)
	if !rs.Reachable || rs.Boot != "2026-11-20T03:00:00Z" || rs.Role != "root" {
		t.Fatalf("%+v", rs)
	}
	var ns networkState
	_ = json.Unmarshal([]byte(b.Last("nestwifi/network/state")), &ns)
	if !ns.WANOnline || ns.DownloadMbps != 40 || ns.ClientsConnected != 3 {
		t.Fatalf("%+v", ns)
	}
	for _, topic := range b.Topics() {
		if payload := b.Last(topic); strings.Contains(payload, "secret") {
			t.Fatal("secret published on", topic)
		}
	}
}
