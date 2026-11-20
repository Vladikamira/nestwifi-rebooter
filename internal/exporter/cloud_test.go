package exporter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeTelemetry struct {
	t   cloud.Telemetry
	err error
}

func (f *fakeTelemetry) Telemetry(context.Context, string) (cloud.Telemetry, error) {
	return f.t, f.err
}

func telemetryFixture() cloud.Telemetry {
	var root, point cloud.AccessPoint
	root.ID, root.Settings.Other.Name = "r", "Living Room Wifi"
	root.Properties.IP, root.Properties.Root, root.Properties.Hardware = "192.0.2.1", true, "H2D"
	point.ID, point.Settings.Other.Name = "p", "Kitchen Wifi"
	point.Properties.IP, point.Properties.Hardware = "192.0.2.32", "H2E"
	return cloud.Telemetry{
		Group:     cloud.Group{ID: "g", AccessPoints: []cloud.AccessPoint{root, point}},
		WANOnline: true,
		APOnline:  map[string]bool{"r": true, "p": true},
		Mesh:      map[string]cloud.MeshSpeed{"p": {Rating: "OK", Score: 50}},
		TxBps:     1e6, RxBps: 4e7,
		Clients: map[cloud.ClientKey]int{{APID: "p", Connection: "WIRELESS", Band: "BAND_5000_MHZ"}: 3},
		Known:   5, Connected: 3,
	}
}

func TestCloudMetrics(t *testing.T) {
	src := &fakeTelemetry{t: telemetryFixture()}
	c := NewCloud(src, "", 0)
	c.poll(context.Background())
	want := `
# HELP nestwifi_mesh_speed_score Mesh link quality to the network (Google numericSpeedRating; OK=50).
# TYPE nestwifi_mesh_speed_score gauge
nestwifi_mesh_speed_score{device="kitchen-wifi",ip="192.0.2.32"} 50
# HELP nestwifi_clients Active clients per access point, connection type and band.
# TYPE nestwifi_clients gauge
nestwifi_clients{band="BAND_5000_MHZ",connection="WIRELESS",device="kitchen-wifi"} 3
# HELP nestwifi_ap_online Access point online state reported by the cloud.
# TYPE nestwifi_ap_online gauge
nestwifi_ap_online{device="kitchen-wifi",hardware="H2E",ip="192.0.2.32",root="false"} 1
nestwifi_ap_online{device="living-room-wifi",hardware="H2D",ip="192.0.2.1",root="true"} 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "nestwifi_mesh_speed_score", "nestwifi_clients", "nestwifi_ap_online"); err != nil {
		t.Fatal(err)
	}

	// A failed poll keeps last values but reports cloud_up=0.
	src.err = errors.New("offline")
	c.poll(context.Background())
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	up := "# HELP nestwifi_cloud_up Last cloud telemetry poll succeeded.\n# TYPE nestwifi_cloud_up gauge\nnestwifi_cloud_up 0\n"
	if err := testutil.GatherAndCompare(reg, strings.NewReader(up), "nestwifi_cloud_up"); err != nil {
		t.Fatal(err)
	}
	if n, err := testutil.GatherAndCount(reg, "nestwifi_mesh_speed_score"); err != nil || n != 1 {
		t.Fatal("last good values lost", n, err)
	}
}

func TestCloudNeverSucceededEmitsOnlyStatus(t *testing.T) {
	c := NewCloud(&fakeTelemetry{err: errors.New("no credentials")}, "", 0)
	c.poll(context.Background())
	if n := testutil.CollectAndCount(c); n != 2 {
		t.Fatalf("expected cloud_up and errors_total only, got %d", n)
	}
}
