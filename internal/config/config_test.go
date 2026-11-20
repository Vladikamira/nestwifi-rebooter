package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const valid = `devices:
  - name: kitchen
    cloud_name: Kitchen Wifi
    kind: wifi_point
    ip: 192.0.2.32
`

func loadTest(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestDefaultsAndCloudName(t *testing.T) {
	c, err := loadTest(t, valid)
	if err != nil {
		t.Fatal(err)
	}
	if c.Rebooter.GroupID != "" || c.Rebooter.QuietSettle.Seconds() != 3 || c.Exporter.Concurrency != 4 {
		t.Fatal("unsafe/unexpected defaults")
	}
	c, err = loadTest(t, strings.ReplaceAll(valid, "    cloud_name: Kitchen Wifi\n", ""))
	if err != nil || c.Devices[0].CloudName != "kitchen" {
		t.Fatal("missing cloud name default")
	}
}

func TestInvalidConfigsRejected(t *testing.T) {
	for name, body := range map[string]string{
		"unknown_field":    valid + "unknown: true\n",
		"hostname":         strings.ReplaceAll(valid, "192.0.2.32", "router.example"),
		"unknown_kind":     strings.ReplaceAll(valid, "wifi_point", "speaker"),
		"duplicate_device": valid + "  - name: kitchen\n    kind: router\n    ip: 192.0.2.1\n",
		"bad_cron":         valid + "rebooter:\n  schedule: '* * * * * *'\n",
		"bad_timezone":     valid + "rebooter:\n  timezone: Not/AZone\n",
		"bad_interval":     valid + "exporter:\n  interval: 0s\n",
		"bad_concurrency":  valid + "exporter:\n  concurrency: 0\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadTest(t, body); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestDiscoveryWithoutStaticDevices(t *testing.T) {
	c, err := loadTest(t, "discovery:\n  enabled: true\n  cidrs: [192.168.86.0/24]\n")
	if err != nil || !c.Discovery.Enabled || len(c.Devices) != 0 {
		t.Fatal("discovery-only config rejected", err)
	}
	for _, cidr := range []string{"8.8.8.0/24", "192.168.0.0/16", "::/0"} {
		if _, err := loadTest(t, "discovery:\n  enabled: true\n  cidrs: ['"+cidr+"']\n"); err == nil {
			t.Fatal("unsafe discovery configuration accepted")
		}
	}
	if _, err := loadTest(t, valid+"discovery:\n  enabled: true\n  cidrs: [192.168.86.0/24]\n"); err == nil {
		t.Fatal("mixed static/discovery configuration accepted")
	}
}
