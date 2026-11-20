package exporter

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
)

// Publisher mirrors exporter state to MQTT with Home Assistant discovery.
// Devices come from the cloud AP list, so every access point appears even when
// it is unreachable on the LAN; local data is merged by IP.
type Publisher struct {
	mqtt     *mqttha.Client
	local    *Collector
	cloud    *CloudCollector
	interval time.Duration

	mu        sync.Mutex
	discovery map[string]string // object ID -> last discovery payload
	state     map[string]string // topic -> last state payload
	boots     map[string]string // AP ID -> last known boot time (kept while unreachable)
}

func NewPublisher(m *mqttha.Client, local *Collector, cloud *CloudCollector, interval time.Duration) *Publisher {
	p := &Publisher{mqtt: m, local: local, cloud: cloud, interval: interval, boots: map[string]string{}}
	p.discovery, p.state = map[string]string{}, map[string]string{}
	// After a (re)connect or HA restart, forget what was sent and republish all.
	m.OnConnect(func() {
		p.mu.Lock()
		p.discovery, p.state = map[string]string{}, map[string]string{}
		p.mu.Unlock()
		p.publish()
	})
	return p
}

type apState struct {
	Online       bool     `json:"online"`
	Reachable    bool     `json:"reachable"`
	Boot         string   `json:"boot,omitempty"`
	MeshRating   string   `json:"mesh_rating,omitempty"`
	MeshScore    *float64 `json:"mesh_score,omitempty"`
	Clients24    int      `json:"clients_2_4ghz"`
	Clients5     int      `json:"clients_5ghz"`
	ClientsWired int      `json:"clients_wired"`
	WiredUplink  *bool    `json:"wired_uplink,omitempty"`
	Role         string   `json:"role,omitempty"`
	Firmware     string   `json:"firmware,omitempty"`
	Update       string   `json:"update,omitempty"`
	LastSeen     string   `json:"last_seen,omitempty"`
}

type networkState struct {
	WANOnline         bool    `json:"wan_online"`
	DownloadMbps      float64 `json:"download_mbps"`
	UploadMbps        float64 `json:"upload_mbps"`
	ClientsConnected  int     `json:"clients_connected"`
	ClientsKnown      int     `json:"clients_known"`
	InventoryComplete bool    `json:"inventory_complete"`
	CloudOK           bool    `json:"cloud_ok"`
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.publish()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, everOK, cloudUp := p.cloud.Last()
	if !everOK {
		return // the AP list (and so the device set) comes from the cloud
	}
	states, disc := p.local.States()
	byIP := map[string]LocalState{}
	for _, s := range states {
		byIP[s.Device.IP] = s
	}
	network := mqttha.ID("nestwifi_network", t.Group.ID)
	for _, ap := range t.Group.AccessPoints {
		p.publishAP(network, ap, t, byIP[ap.Properties.IP])
	}
	ns := networkState{WANOnline: t.WANOnline, DownloadMbps: round(t.RxBps / 1e6), UploadMbps: round(t.TxBps / 1e6),
		ClientsConnected: t.Connected, ClientsKnown: t.Known, InventoryComplete: disc.Complete, CloudOK: cloudUp}
	topic := "network/state"
	p.send(mqttha.Device{
		ObjectID: network,
		Dev:      mqttha.DeviceInfo{IDs: []string{network}, Name: "Nest Wifi", Manufacturer: "Google", Model: "Wifi network"},
		Components: map[string]mqttha.Component{
			"wan":       mqttha.Binary(network+"_wan", "Internet", "wan_online", "connectivity"),
			"download":  mqttha.Sensor(network+"_download", "Download", "{{ value_json.download_mbps }}").Class("data_rate", "measurement", "Mbit/s"),
			"upload":    mqttha.Sensor(network+"_upload", "Upload", "{{ value_json.upload_mbps }}").Class("data_rate", "measurement", "Mbit/s"),
			"clients":   mqttha.Sensor(network+"_clients", "Connected clients", "{{ value_json.clients_connected }}").Class("", "measurement", "").WithIcon("mdi:devices"),
			"known":     mqttha.Sensor(network+"_known", "Known clients", "{{ value_json.clients_known }}").Diagnostic(),
			"inventory": mqttha.Binary(network+"_inventory", "Inventory complete", "inventory_complete", "").Diagnostic(),
			"cloud":     mqttha.Binary(network+"_cloud", "Cloud reachable", "cloud_ok", "connectivity").Diagnostic(),
		},
	}, topic, ns)
}

func (p *Publisher) publishAP(network string, ap cloud.AccessPoint, t cloud.Telemetry, local LocalState) {
	props := ap.Properties
	id := mqttha.ID("nestwifi_ap", ap.ID)
	s := apState{Online: t.APOnline[ap.ID], Reachable: local.OK, Firmware: props.Firmware}
	if !props.LastSeen.IsZero() {
		s.LastSeen = props.LastSeen.UTC().Format(time.RFC3339)
	}
	for k, n := range t.Clients {
		if k.APID != ap.ID {
			continue
		}
		switch k.Band {
		case "BAND_2400_MHZ":
			s.Clients24 += n
		case "BAND_5000_MHZ":
			s.Clients5 += n
		default:
			s.ClientsWired += n
		}
	}
	if m, ok := t.Mesh[ap.ID]; ok {
		s.MeshRating, s.MeshScore = m.Rating, &m.Score
	}
	if local.OK {
		snap := local.Snapshot
		p.boots[ap.ID] = snap.Boot.UTC().Truncate(time.Minute).Format(time.RFC3339)
		s.Role, s.Update = snap.Role, snap.Update
		if snap.Firmware != "" {
			s.Firmware = snap.Firmware
		}
		if snap.Backhaul != "" {
			wired := snap.Backhaul == "wired"
			s.WiredUplink = &wired
		}
	}
	s.Boot = p.boots[ap.ID] // last known boot stays valid while unreachable

	model := map[string]string{"H2D": "Nest Wifi router", "H2E": "Nest Wifi point"}[props.Hardware]
	if model == "" {
		model = props.Hardware
	}
	c := map[string]mqttha.Component{
		"online":    mqttha.Binary(id+"_online", "Online", "online", "connectivity"),
		"reachable": mqttha.Binary(id+"_reachable", "Local API", "reachable", "connectivity").Diagnostic(),
		"clients":   mqttha.Sensor(id+"_clients", "Clients", "{{ value_json.clients_2_4ghz + value_json.clients_5ghz + value_json.clients_wired }}").Class("", "measurement", "").WithIcon("mdi:devices"),
		"clients24": mqttha.Sensor(id+"_clients24", "Clients 2.4 GHz", "{{ value_json.clients_2_4ghz }}").Class("", "measurement", "").Diagnostic(),
		"clients5":  mqttha.Sensor(id+"_clients5", "Clients 5 GHz", "{{ value_json.clients_5ghz }}").Class("", "measurement", "").Diagnostic(),
		"firmware":  mqttha.Sensor(id+"_firmware", "Firmware", "{{ value_json.firmware }}").Diagnostic(),
		"last_seen": mqttha.Sensor(id+"_last_seen", "Last seen by cloud", "{{ value_json.last_seen }}").Class("timestamp", "", "").Diagnostic().Disabled(),
	}
	// Only fields this device has, so templates never reference missing keys.
	if s.Boot != "" {
		c["boot"] = mqttha.Sensor(id+"_boot", "Last boot", "{{ value_json.boot }}").Class("timestamp", "", "")
	}
	if s.MeshScore != nil {
		c["mesh"] = mqttha.Sensor(id+"_mesh", "Mesh quality", "{{ value_json.mesh_rating }}").WithIcon("mdi:wifi-strength-3")
		c["mesh_score"] = mqttha.Sensor(id+"_mesh_score", "Mesh score", "{{ value_json.mesh_score }}").Class("", "measurement", "").Diagnostic()
	}
	if s.WiredUplink != nil {
		c["wired_uplink"] = mqttha.Binary(id+"_wired_uplink", "Wired uplink", "wired_uplink", "").Diagnostic().WithIcon("mdi:ethernet")
	}
	if s.Role != "" {
		c["role"] = mqttha.Sensor(id+"_role", "Role", "{{ value_json.role }}").Diagnostic()
		c["update"] = mqttha.Sensor(id+"_update", "Update status", "{{ value_json.update }}").Diagnostic()
	}
	if s.ClientsWired > 0 || props.Hardware == "H2D" {
		c["clients_wired"] = mqttha.Sensor(id+"_clients_wired", "Clients wired", "{{ value_json.clients_wired }}").Class("", "measurement", "").Diagnostic()
	}
	p.send(mqttha.Device{
		ObjectID: id,
		Dev: mqttha.DeviceInfo{IDs: []string{id}, Name: ap.Name(), Manufacturer: "Google", Model: model,
			Software: s.Firmware, ViaDevice: network},
		Components: c,
	}, "ap/"+discovery.Slug(ap.Name())+"/state", s)
}

// send publishes discovery and state, each only when changed.
func (p *Publisher) send(d mqttha.Device, stateTopic string, state any) {
	d.StateTopic = p.mqtt.Topic(stateTopic)
	d.Availability = p.mqtt.Availability()
	d.Origin = map[string]string{"name": "nestwifi-exporter"}
	if b, _ := json.Marshal(d); p.discovery[d.ObjectID] != string(b) {
		p.mqtt.Discover(d)
		p.discovery[d.ObjectID] = string(b)
	}
	if b, _ := json.Marshal(state); p.state[stateTopic] != string(b) {
		p.mqtt.Publish(stateTopic, state)
		p.state[stateTopic] = string(b)
	}
}

func round(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }
