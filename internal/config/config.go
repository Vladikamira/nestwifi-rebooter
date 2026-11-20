package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

type Device struct {
	Name      string `yaml:"name" json:"name"`
	IP        string `yaml:"ip" json:"ip"`
	Kind      string `yaml:"kind" json:"kind"`
	CloudName string `yaml:"cloud_name" json:"cloud_name"`
	CloudID   string `yaml:"cloud_id" json:"cloud_id,omitempty"`
	LocalID   string `yaml:"local_id" json:"local_id,omitempty"`
}

// Key is the most stable identity known for a device. LocalID (router
// hardwareId or Point cloud_device_id) is available even without the cloud.
func (d Device) Key() string {
	switch {
	case d.LocalID != "":
		return d.Kind + ":local:" + d.LocalID
	case d.CloudID != "":
		return d.Kind + ":cloud:" + d.CloudID
	}
	return d.Kind + ":ip:" + d.IP
}

type Discovery struct {
	Enabled     bool          `yaml:"enabled"`
	CIDRs       []string      `yaml:"cidrs"`
	Interval    time.Duration `yaml:"interval"`
	Timeout     time.Duration `yaml:"timeout"`
	Concurrency int           `yaml:"concurrency"`
}

// MQTT is disabled when URL is empty. The password is read from
// MQTT_PASSWORD_FILE (preferred) or MQTT_PASSWORD, never from this file.
type MQTT struct {
	URL             string `yaml:"url"` // tcp://host:1883, ssl://host:8883
	Username        string `yaml:"username"`
	ClientID        string `yaml:"client_id"` // default nestwifi-<binary>
	TopicPrefix     string `yaml:"topic_prefix"`
	DiscoveryPrefix string `yaml:"discovery_prefix"`
	TLS             struct {
		CAFile             string `yaml:"ca_file"`
		CertFile           string `yaml:"cert_file"`
		KeyFile            string `yaml:"key_file"`
		InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	} `yaml:"tls"`
}

type Config struct {
	Listen    string    `yaml:"listen"`
	HomeID    string    `yaml:"home_id"`
	Devices   []Device  `yaml:"devices"`
	Discovery Discovery `yaml:"discovery"`
	MQTT      MQTT      `yaml:"mqtt"`
	Exporter  struct {
		Interval    time.Duration `yaml:"interval"`
		Timeout     time.Duration `yaml:"timeout"`
		Concurrency int           `yaml:"concurrency"`
		Cast        bool          `yaml:"cast"`
		// Cloud telemetry (mesh quality, AP state, clients, traffic); 0 disables.
		CloudInterval time.Duration `yaml:"cloud_interval"`
	} `yaml:"exporter"`
	Rebooter struct {
		Schedule         string        `yaml:"schedule"`
		Timezone         string        `yaml:"timezone"`
		GroupID          string        `yaml:"group_id"`
		StateDir         string        `yaml:"state_dir"`
		WaitTimeout      time.Duration `yaml:"wait_timeout"`
		PollInterval     time.Duration `yaml:"poll_interval"`
		Settle           time.Duration `yaml:"settle"`
		QuietSettle      time.Duration `yaml:"quiet_settle"`
		RecoveryInterval time.Duration `yaml:"recovery_interval"`
		// Minimum time between two network reboots, also for Home Assistant presses.
		MinInterval time.Duration `yaml:"min_interval"`
		// Speedtest after a successful network reboot; 0 disables.
		SpeedtestDelay time.Duration `yaml:"speedtest_delay"`
		// Clients (exact Google Home names or station IDs) that Home Assistant may
		// pause or prioritize. Empty: no client controls are exposed.
		ManagedClients []string `yaml:"managed_clients"`
	} `yaml:"rebooter"`
}

func Load(path string) (Config, error) {
	var c Config
	c.Listen = ":9109"
	c.Discovery.Interval = 2 * time.Minute
	c.Discovery.Timeout = 1500 * time.Millisecond
	c.Discovery.Concurrency = 32
	c.Exporter.Interval = 30 * time.Second
	c.Exporter.Timeout = 15 * time.Second
	c.Exporter.Concurrency = 4
	c.Exporter.Cast = true
	c.Exporter.CloudInterval = 2 * time.Minute // Google rate-limits (HTTP 429)
	c.Rebooter.Schedule = "0 3 * * *"
	c.Rebooter.Timezone = "Etc/UTC"
	c.Rebooter.StateDir = "/state"
	c.Rebooter.WaitTimeout = 10 * time.Minute
	c.Rebooter.PollInterval = 5 * time.Second
	c.Rebooter.Settle = 30 * time.Second
	c.Rebooter.QuietSettle = 3 * time.Second
	c.Rebooter.RecoveryInterval = time.Minute
	c.Rebooter.MinInterval = time.Hour
	c.Rebooter.SpeedtestDelay = 10 * time.Minute
	c.MQTT.TopicPrefix = "nestwifi"
	c.MQTT.DiscoveryPrefix = "homeassistant"
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if len(c.Devices) == 0 && !c.Discovery.Enabled {
		return c, errors.New("devices or enabled discovery are required")
	}
	if c.Discovery.Enabled {
		if len(c.Devices) > 0 {
			return c, errors.New("use either discovery or static devices, not both")
		}
		d := c.Discovery
		if len(d.CIDRs) == 0 || len(d.CIDRs) > 4 || d.Interval <= 0 || d.Timeout <= 0 || d.Concurrency < 1 || d.Concurrency > 64 {
			return c, errors.New("invalid discovery configuration")
		}
		total := 0
		for _, cidr := range c.Discovery.CIDRs {
			ip, network, err := net.ParseCIDR(cidr)
			if err != nil || ip.To4() == nil || !ip.IsPrivate() {
				return c, errors.New("discovery requires private IPv4 CIDRs")
			}
			ones, bits := network.Mask.Size()
			if bits != 32 || ones < 22 {
				return c, errors.New("discovery CIDRs must be /22 or smaller")
			}
			total += 1 << uint(32-ones)
		}
		if total > 1024 {
			return c, errors.New("discovery is limited to 1024 addresses total")
		}
	}
	seen := map[string]bool{}
	ips := map[string]bool{}
	for i := range c.Devices {
		v := &c.Devices[i]
		if v.Name == "" || seen[v.Name] {
			return c, errors.New("device names must be nonempty and unique")
		}
		if net.ParseIP(v.IP) == nil || ips[v.IP] {
			return c, fmt.Errorf("device %s requires a unique literal IP", v.Name)
		}
		if v.Kind != "router" && v.Kind != "wifi_point" {
			return c, fmt.Errorf("invalid kind for %s", v.Name)
		}
		if v.CloudName == "" {
			v.CloudName = v.Name
		}
		seen[v.Name] = true
		ips[v.IP] = true
	}
	e := c.Exporter
	if e.Interval <= 0 || e.Timeout <= 0 || e.Concurrency < 1 || e.Concurrency > 64 || e.CloudInterval < 0 || e.CloudInterval > 0 && e.CloudInterval < time.Minute {
		return c, errors.New("invalid exporter timing/concurrency")
	}
	r := c.Rebooter
	if r.WaitTimeout <= 0 || r.PollInterval <= 0 || r.Settle < 0 || r.QuietSettle < 0 || r.RecoveryInterval <= 0 || r.StateDir == "" ||
		r.MinInterval < 10*time.Minute || r.SpeedtestDelay < 0 {
		return c, errors.New("invalid rebooter timing/state directory (min_interval is at least 10m)")
	}
	if m := c.MQTT; m.URL != "" {
		u, err := url.Parse(m.URL)
		if err != nil || u.Host == "" || !slices.Contains([]string{"tcp", "mqtt", "ssl", "tls", "mqtts"}, u.Scheme) {
			return c, errors.New("mqtt.url must be tcp://host:port or ssl://host:port")
		}
		if m.TopicPrefix == "" || m.DiscoveryPrefix == "" || strings.ContainsAny(m.TopicPrefix+m.DiscoveryPrefix, "#+ ") {
			return c, errors.New("invalid MQTT topic prefixes")
		}
	}
	if _, err := time.LoadLocation(r.Timezone); err != nil {
		return c, errors.New("invalid timezone")
	}
	if _, err := cron.ParseStandard(r.Schedule); err != nil {
		return c, errors.New("schedule must be five-field cron")
	}
	return c, nil
}
