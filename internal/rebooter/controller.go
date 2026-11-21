package rebooter

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
)

// Actions are the cloud operations exposed to Home Assistant.
type Actions interface {
	Group(ctx context.Context, id string) (cloud.Group, error)
	SetLight(ctx context.Context, apID string, intensity int) error
	RunSpeedTest(ctx context.Context, groupID string) (cloud.SpeedTest, error)
	Clients(ctx context.Context, groupID string) ([]cloud.Client, error)
	SetPaused(ctx context.Context, groupID, stationID string, paused bool) error
	Prioritize(ctx context.Context, groupID, stationID string, hours int) error
	ClearPriority(ctx context.Context, groupID string) error
}

// Controller exposes the rebooter and other actions to Home Assistant via
// MQTT. Commands run one at a time in the background; results are reported
// through state topics, never by retrying a command.
type Controller struct {
	ctx     context.Context
	runner  *Runner
	cloud   Actions
	mqtt    *mqttha.Client
	groupID string
	managed []string
	refresh time.Duration

	busy sync.Mutex // one cloud action at a time (besides the runner's own lock)

	mu      sync.Mutex
	aps     []cloud.AccessPoint
	clients []cloud.Client
	speed   cloud.SpeedTest
	speedOK bool
	speedEr string

	download, upload, speedAt prometheus.Gauge
}

func NewController(ctx context.Context, r *Runner, a Actions, groupID string, managed []string, registry *prometheus.Registry) *Controller {
	c := &Controller{ctx: ctx, runner: r, cloud: a, groupID: groupID, managed: managed, refresh: 5 * time.Minute}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "nestwifi_speedtest_" + name, Help: help})
		registry.MustRegister(g)
		return g
	}
	c.download = gauge("download_bits_per_second", "Last WAN speed test download.")
	c.upload = gauge("upload_bits_per_second", "Last WAN speed test upload.")
	c.speedAt = gauge("timestamp_seconds", "Time of the last WAN speed test.")
	return c
}

// SpeedTest runs one WAN speed test and records the result.
func (c *Controller) SpeedTest(ctx context.Context) error {
	if !c.busy.TryLock() {
		return errors.New("another action is running")
	}
	defer c.busy.Unlock()
	task, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	s, err := c.cloud.RunSpeedTest(task, c.groupID)
	c.mu.Lock()
	if err == nil {
		c.speed, c.speedOK, c.speedEr = s, true, ""
		c.download.Set(s.DownloadBps)
		c.upload.Set(s.UploadBps)
		c.speedAt.Set(float64(s.At.Unix()))
		slog.Info("speed test", "download_mbps", s.DownloadBps/1e6, "upload_mbps", s.UploadBps/1e6)
	} else {
		c.speedEr = err.Error()
		slog.Warn("speed test failed", "error", err)
	}
	c.mu.Unlock()
	c.publishState()
	return err
}

// AfterReboot waits for the network to settle, then runs a speed test.
func (c *Controller) AfterReboot(delay time.Duration) func(context.Context) {
	return func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-time.After(delay):
			_ = c.SpeedTest(ctx)
		}
	}
}

// Attach wires MQTT: discovery, state and command handlers.
func (c *Controller) Attach(m *mqttha.Client) {
	c.mqtt = m
	c.runner.OnChange(func(Status) { c.publishState() })
	m.Subscribe("rebooter/reboot", func([]byte) { c.do("network reboot", c.runner.Run) })
	m.Subscribe("rebooter/speedtest", func([]byte) { c.do("speed test", c.SpeedTest) })
	m.Subscribe("rebooter/schedule/set", func(p []byte) {
		if err := c.runner.SetSchedule(string(p) == "ON"); err != nil {
			slog.Error("cannot store schedule switch", "error", err)
		}
	})
	// Per-AP and per-client commands are subscribed in announce, once their IDs are known.
	m.OnConnect(func() { c.reload(); c.announce() })
}

// do runs an action in the background so MQTT handlers never block.
func (c *Controller) do(name string, f func(context.Context) error) {
	go func() {
		slog.Info("Home Assistant command", "action", name)
		if err := f(c.ctx); err != nil {
			slog.Warn("Home Assistant command failed", "action", name, "error", err)
		}
	}()
}

// Run periodically reloads lights and managed clients from the cloud.
func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(c.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reload()
			c.publishState()
		}
	}
}

func (c *Controller) reload() {
	ctx, cancel := context.WithTimeout(c.ctx, time.Minute)
	defer cancel()
	g, err := c.cloud.Group(ctx, c.groupID)
	if err != nil {
		slog.Warn("cannot load access points", "error", err)
		return
	}
	var clients []cloud.Client
	if len(c.managed) > 0 {
		all, err := c.cloud.Clients(ctx, g.ID)
		if err != nil {
			slog.Warn("cannot load clients", "error", err)
		}
		for _, cl := range all {
			if slices.Contains(c.managed, cl.ID) || slices.Contains(c.managed, cl.Name) {
				clients = append(clients, cl)
			}
		}
	}
	c.mu.Lock()
	c.aps, c.clients = g.AccessPoints, clients
	if c.groupID == "" {
		c.groupID = g.ID
	}
	c.mu.Unlock()
}

func (c *Controller) network() string { return mqttha.ID("nestwifi_network", c.groupID) }

type lightState struct {
	State      string `json:"state"`
	Brightness int    `json:"brightness"`
}

type clientState struct {
	Internet  bool `json:"internet"`
	Connected bool `json:"connected"`
}

// announce publishes discovery and subscribes per-AP and per-client commands.
func (c *Controller) announce() {
	if c.mqtt == nil {
		return
	}
	c.mu.Lock()
	aps, clients := slices.Clone(c.aps), slices.Clone(c.clients)
	c.mu.Unlock()
	m, network := c.mqtt, c.network()

	// JSON null renders as "None", which HA MQTT sensors treat as unknown.
	cmps := map[string]mqttha.Component{
		"reboot": {Platform: "button", UniqueID: network + "_quiet_reboot", Name: "Quiet reboot",
			CommandTopic: m.Topic("rebooter/reboot"), PayloadPress: "PRESS", DeviceClass: "restart"},
		"schedule": {Platform: "switch", UniqueID: network + "_schedule", Name: "Scheduled reboot",
			CommandTopic: m.Topic("rebooter/schedule/set"), ValueTemplate: "{{ 'ON' if value_json.schedule else 'OFF' }}",
			PayloadOn: "ON", PayloadOff: "OFF", Icon: "mdi:calendar-clock"},
		"result":       mqttha.Sensor(network+"_reboot_result", "Last reboot result", "{{ value_json.last_result }}").WithIcon("mdi:restart"),
		"error":        mqttha.Sensor(network+"_reboot_error", "Last reboot problem", "{{ value_json.last_error }}").WithIcon("mdi:alert-circle-outline").Diagnostic(),
		"last_success": mqttha.Sensor(network+"_reboot_last_success", "Last successful reboot", "{{ value_json.last_success }}").Class("timestamp", "", ""),
		"next_run":     mqttha.Sensor(network+"_reboot_next", "Next scheduled reboot", "{{ value_json.next_run }}").Class("timestamp", "", ""),
		"active":       mqttha.Binary(network+"_reboot_active", "Reboot in progress", "active", "running"),
		"pending":      mqttha.Binary(network+"_restore_pending", "Settings restore pending", "restore_pending", "problem"),
		"enabled":      mqttha.Binary(network+"_reboot_enabled", "Reboot enabled", "enabled", "").Diagnostic(),
		"speedtest": {Platform: "button", UniqueID: network + "_speedtest", Name: "Run speed test",
			CommandTopic: m.Topic("rebooter/speedtest"), PayloadPress: "PRESS", Icon: "mdi:speedometer"},
		"speed_down": mqttha.Sensor(network+"_speed_down", "Speed test download", "{{ value_json.speed_download_mbps }}").Class("data_rate", "measurement", "Mbit/s"),
		"speed_up":   mqttha.Sensor(network+"_speed_up", "Speed test upload", "{{ value_json.speed_upload_mbps }}").Class("data_rate", "measurement", "Mbit/s"),
		"speed_at":   mqttha.Sensor(network+"_speed_at", "Speed test time", "{{ value_json.speed_at }}").Class("timestamp", "", "").Diagnostic(),
	}
	if len(clients) > 0 {
		cmps["clear_priority"] = mqttha.Component{Platform: "button", UniqueID: network + "_clear_priority", Name: "Clear client priority",
			CommandTopic: m.Topic("rebooter/priority/clear"), PayloadPress: "PRESS", Icon: "mdi:priority-low"}
		m.Subscribe("rebooter/priority/clear", func([]byte) {
			c.do("clear priority", func(ctx context.Context) error { return c.cloud.ClearPriority(ctx, c.groupID) })
		})
	}
	for _, cl := range clients {
		slug := discovery.Slug(cl.Name)
		if slug == "" {
			slug = mqttha.ID(cl.ID)
		}
		base := "rebooter/client/" + slug
		cmps["internet_"+slug] = mqttha.Component{Platform: "switch", UniqueID: mqttha.ID(network, "client", cl.ID, "internet"),
			Name: "Internet " + cl.Name, StateTopic: m.Topic(base + "/state"), CommandTopic: m.Topic(base + "/set"),
			ValueTemplate: "{{ 'ON' if value_json.internet else 'OFF' }}", PayloadOn: "ON", PayloadOff: "OFF", Icon: "mdi:web"}
		cmps["prioritize_"+slug] = mqttha.Component{Platform: "button", UniqueID: mqttha.ID(network, "client", cl.ID, "prioritize"),
			Name: "Prioritize " + cl.Name + " 1h", CommandTopic: m.Topic(base + "/prioritize"), PayloadPress: "PRESS", Icon: "mdi:priority-high"}
		id := cl.ID
		m.Subscribe(base+"/set", func(p []byte) {
			c.do("internet "+slug, func(ctx context.Context) error {
				err := c.cloud.SetPaused(ctx, c.groupID, id, string(p) == "OFF")
				c.reload()
				c.publishState()
				return err
			})
		})
		m.Subscribe(base+"/prioritize", func([]byte) {
			c.do("prioritize "+slug, func(ctx context.Context) error { return c.cloud.Prioritize(ctx, c.groupID, id, 1) })
		})
	}
	m.Discover(mqttha.Device{ObjectID: network + "_rebooter", Dev: mqttha.DeviceInfo{IDs: []string{network}},
		Origin: map[string]string{"name": "nestwifi-rebooter"}, Components: cmps,
		StateTopic: m.Topic("rebooter/state"), Availability: m.Availability()})

	for _, ap := range aps {
		id, slug := mqttha.ID("nestwifi_ap", ap.ID), discovery.Slug(ap.Name())
		base := "rebooter/ap/" + slug
		m.Discover(mqttha.Device{ObjectID: id + "_rebooter", Dev: mqttha.DeviceInfo{IDs: []string{id}},
			Origin: map[string]string{"name": "nestwifi-rebooter"}, Availability: m.Availability(),
			Components: map[string]mqttha.Component{
				"restart": {Platform: "button", UniqueID: id + "_quiet_restart", Name: "Quiet restart",
					CommandTopic: m.Topic(base + "/reboot"), PayloadPress: "PRESS", DeviceClass: "restart"},
				"light": {Platform: "light", UniqueID: id + "_light", Name: "Status light", Schema: "json",
					StateTopic: m.Topic(base + "/light"), CommandTopic: m.Topic(base + "/light/set"),
					Brightness: true, BrightnessScale: 100, SupportedModes: []string{"brightness"}},
			}})
		apID := ap.ID
		m.Subscribe(base+"/reboot", func([]byte) {
			c.do("restart "+slug, func(ctx context.Context) error { return c.runner.RebootAP(ctx, apID) })
		})
		m.Subscribe(base+"/light/set", func(p []byte) {
			var cmd struct {
				State      string `json:"state"`
				Brightness *int   `json:"brightness"`
			}
			if json.Unmarshal(p, &cmd) != nil {
				slog.Warn("invalid light command", "device", slug)
				return
			}
			c.do("light "+slug, func(ctx context.Context) error {
				level := 0
				if cmd.State == "ON" {
					level = 100
					if cmd.Brightness != nil {
						level = *cmd.Brightness
					}
				}
				err := c.cloud.SetLight(ctx, apID, level)
				if err == nil {
					c.mu.Lock()
					for i := range c.aps {
						if c.aps[i].ID == apID {
							c.aps[i].Settings.Lighting.Intensity = level
						}
					}
					c.mu.Unlock()
					c.publishState()
				}
				return err
			})
		})
	}
	c.publishState()
}

func (c *Controller) publishState() {
	if c.mqtt == nil {
		return
	}
	st := c.runner.Status()
	c.mu.Lock()
	state := map[string]any{"schedule": st.Schedule, "active": st.Active, "restore_pending": st.RestorePending,
		"enabled": st.Enabled, "last_result": st.LastResult, "last_error": st.LastError}
	stamp := func(key string, t time.Time) {
		if !t.IsZero() {
			state[key] = t.UTC().Format(time.RFC3339)
		} else {
			state[key] = nil
		}
	}
	stamp("last_success", st.LastSuccess)
	stamp("last_attempt", st.LastAttempt)
	stamp("next_run", st.NextRun)
	if c.speedOK {
		state["speed_download_mbps"] = float64(int64(c.speed.DownloadBps/1e5+0.5)) / 10
		state["speed_upload_mbps"] = float64(int64(c.speed.UploadBps/1e5+0.5)) / 10
		stamp("speed_at", c.speed.At)
	} else {
		state["speed_download_mbps"], state["speed_upload_mbps"], state["speed_at"] = nil, nil, nil
	}
	if c.speedEr != "" {
		state["speed_error"] = c.speedEr
	}
	aps, clients := slices.Clone(c.aps), slices.Clone(c.clients)
	c.mu.Unlock()

	c.mqtt.Publish("rebooter/state", state)
	for _, ap := range aps {
		l := lightState{State: "OFF", Brightness: ap.Settings.Lighting.Intensity}
		if l.Brightness > 0 {
			l.State = "ON"
		}
		c.mqtt.Publish("rebooter/ap/"+discovery.Slug(ap.Name())+"/light", l)
	}
	for _, cl := range clients {
		slug := discovery.Slug(cl.Name)
		if slug == "" {
			slug = mqttha.ID(cl.ID)
		}
		c.mqtt.Publish("rebooter/client/"+slug+"/state", clientState{Internet: !cl.Paused, Connected: cl.Connected})
	}
}
