package rebooter

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha/mqtttest"
)

type fakeActions struct {
	mu     sync.Mutex
	lights map[string]int
	paused map[string]bool
	prio   []string
	speeds int
}

func (f *fakeActions) Group(context.Context, string) (cloud.Group, error) {
	var ap cloud.AccessPoint
	ap.ID, ap.Settings.Other.Name, ap.Properties.IP = "ap-kitchen", "Kitchen Wifi", "192.0.2.2"
	ap.Settings.Lighting.Intensity = 100
	return cloud.Group{ID: "group", AccessPoints: []cloud.AccessPoint{ap}}, nil
}
func (f *fakeActions) SetLight(_ context.Context, apID string, v int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lights[apID] = v
	return nil
}
func (f *fakeActions) RunSpeedTest(context.Context, string) (cloud.SpeedTest, error) {
	f.mu.Lock()
	f.speeds++
	f.mu.Unlock()
	return cloud.SpeedTest{At: time.Date(2026, 11, 20, 9, 41, 0, 0, time.UTC), DownloadBps: 412e6, UploadBps: 386e6}, nil
}
func (f *fakeActions) Clients(context.Context, string) ([]cloud.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []cloud.Client{
		{ID: "st-1", Name: "Kids Switch", Connected: true, Paused: f.paused["st-1"]},
		{ID: "st-2", Name: "Not managed", Connected: true},
	}, nil
}
func (f *fakeActions) SetPaused(_ context.Context, _, id string, p bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused[id] = p
	return nil
}
func (f *fakeActions) Prioritize(_ context.Context, _, id string, _ int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prio = append(f.prio, id)
	return nil
}
func (f *fakeActions) ClearPriority(context.Context, string) error { return nil }

func TestControllerHomeAssistant(t *testing.T) {
	r, fc, _ := fixture(t, true)
	r.c.Rebooter.MinInterval = time.Hour
	// The runner holds r.mu while it works, so locking it orders our reads.
	calls := func() int { r.mu.Lock(); defer r.mu.Unlock(); return fc.calls }
	b := mqtttest.Start(t)
	m, err := mqttha.Connect(config.MQTT{URL: b.URL, TopicPrefix: "nestwifi", DiscoveryPrefix: "homeassistant"}, "rebooter")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	actions := &fakeActions{lights: map[string]int{}, paused: map[string]bool{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewController(ctx, r, actions, "group", []string{"Kids Switch"}, prometheus.NewRegistry())
	c.Attach(m)

	disc := "homeassistant/device/nestwifi_network_group_rebooter/config"
	mqtttest.Wait(t, func() bool {
		return b.Last(disc) != "" && b.Last("homeassistant/device/nestwifi_ap_ap-kitchen_rebooter/config") != ""
	})
	if d := b.Last(disc); !strings.Contains(d, `"Quiet reboot"`) || !strings.Contains(d, "Internet Kids Switch") || strings.Contains(d, "Not managed") {
		t.Fatal("unexpected discovery:", d)
	}

	// Light: HA JSON schema command.
	b.Publish("nestwifi/rebooter/ap/kitchen-wifi/light/set", []byte(`{"state":"ON","brightness":30}`), false, 1)
	mqtttest.Wait(t, func() bool { actions.mu.Lock(); defer actions.mu.Unlock(); return actions.lights["ap-kitchen"] == 30 })
	mqtttest.Wait(t, func() bool {
		return strings.Contains(b.Last("nestwifi/rebooter/ap/kitchen-wifi/light"), `"brightness":30`)
	})
	b.Publish("nestwifi/rebooter/ap/kitchen-wifi/light/set", []byte(`{"state":"OFF"}`), false, 1)
	mqtttest.Wait(t, func() bool { actions.mu.Lock(); defer actions.mu.Unlock(); return actions.lights["ap-kitchen"] == 0 })

	// Pause a managed client.
	b.Publish("nestwifi/rebooter/client/kids-switch/set", []byte("OFF"), false, 1)
	mqtttest.Wait(t, func() bool {
		return strings.Contains(b.Last("nestwifi/rebooter/client/kids-switch/state"), `"internet":false`)
	})

	// Speed test.
	b.Publish("nestwifi/rebooter/speedtest", []byte("PRESS"), false, 1)
	mqtttest.Wait(t, func() bool { return strings.Contains(b.Last("nestwifi/rebooter/state"), `"speed_download_mbps":412`) })

	// Network reboot: exactly one cloud call, result published.
	b.Publish("nestwifi/rebooter/reboot", []byte("PRESS"), false, 1)
	mqtttest.Wait(t, func() bool { return strings.Contains(b.Last("nestwifi/rebooter/state"), `"last_result":"success"`) })
	if n := calls(); n != 1 {
		t.Fatalf("reboot calls=%d", n)
	}
	// A second press within min_interval is refused.
	b.Publish("nestwifi/rebooter/reboot", []byte("PRESS"), false, 1)
	mqtttest.Wait(t, func() bool { return strings.Contains(b.Last("nestwifi/rebooter/state"), `"last_result":"too_soon"`) })
	if calls() != 1 {
		t.Fatal("second press rebooted again")
	}

	// Schedule switch is persisted.
	b.Publish("nestwifi/rebooter/schedule/set", []byte("OFF"), false, 1)
	mqtttest.Wait(t, func() bool { return !r.store.ScheduleEnabled() })
	var st map[string]any
	mqtttest.Wait(t, func() bool {
		_ = json.Unmarshal([]byte(b.Last("nestwifi/rebooter/state")), &st)
		return st["schedule"] == false
	})
}
