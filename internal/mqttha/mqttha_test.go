package mqttha

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha/mqtttest"
)

func testConfig(url string) config.MQTT {
	return config.MQTT{URL: url, TopicPrefix: "nestwifi", DiscoveryPrefix: "homeassistant"}
}

func TestAvailabilityDiscoveryAndCommands(t *testing.T) {
	b := mqtttest.Start(t)
	var presses atomic.Int32
	// A command left retained on the broker is replayed on every (re)subscribe;
	// it must never trigger an action.
	b.Publish("nestwifi/cmd", []byte("PRESS"), true, 1)

	c, err := Connect(testConfig(b.URL), "test")
	if err != nil {
		t.Fatal(err)
	}
	announced := make(chan struct{}, 4)
	c.OnConnect(func() {
		c.Discover(Device{ObjectID: "dev1", Dev: DeviceInfo{IDs: []string{"dev1"}}, Components: map[string]Component{"x": Sensor("dev1_x", "X", "{{ value_json.x }}")}})
		announced <- struct{}{}
	})
	c.Subscribe("cmd", func([]byte) { presses.Add(1) })

	mqtttest.Wait(t, func() bool { return b.Last("nestwifi/test/status") == "online" })
	<-announced
	mqtttest.Wait(t, func() bool { return b.Last("homeassistant/device/dev1/config") != "" })

	// A Home Assistant restart triggers a new announcement.
	b.Publish("homeassistant/status", []byte("online"), false, 1)
	select {
	case <-announced:
	case <-time.After(5 * time.Second):
		t.Fatal("no re-announce after HA birth")
	}

	b.Publish("nestwifi/cmd", []byte("PRESS"), false, 1)
	mqtttest.Wait(t, func() bool { return presses.Load() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := presses.Load(); n != 1 {
		t.Fatalf("presses=%d; the replayed retained command must be ignored", n)
	}

	c.Close()
	mqtttest.Wait(t, func() bool { return b.Last("nestwifi/test/status") == "offline" })
}

func TestTLSFilesValidated(t *testing.T) {
	c := testConfig("ssl://127.0.0.1:8883")
	c.TLS.CAFile = "/nonexistent/ca.pem"
	if _, err := Connect(c, "test"); err == nil {
		t.Fatal("missing CA file accepted")
	}
}

func TestPasswordFromFile(t *testing.T) {
	path := t.TempDir() + "/pw"
	if err := os.WriteFile(path, []byte(" secret \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MQTT_PASSWORD_FILE", path)
	if pw, err := password(); err != nil || pw != "secret" {
		t.Fatal("password file not read", pw, err)
	}
}

func TestID(t *testing.T) {
	if got := ID("nestwifi_ap", "a/b c#d"); got != "nestwifi_ap_a_b_c_d" {
		t.Fatal(got)
	}
}
