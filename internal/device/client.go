package device

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

type Tokens interface {
	LocalToken(context.Context, config.Device, bool) (string, error)
}
type Client struct {
	Tokens Tokens
	HTTP   *http.Client
}

func New(tokens Tokens) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	// LAN devices use self-signed certificates; this client never talks to the cloud.
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{Tokens: tokens, HTTP: &http.Client{Transport: tr, Timeout: 15 * time.Second, CheckRedirect: noRedirect}}
}

type Window struct {
	Days   []int   `json:"days"`
	Start  float64 `json:"start_hour"`
	Length float64 `json:"length_hours"`
}
type Night struct {
	Enabled    bool     `json:"enabled"`
	DND        bool     `json:"do_not_disturb"`
	Brightness float64  `json:"led_brightness"`
	Volume     float64  `json:"volume"`
	Windows    []Window `json:"windows"`
}

func (n Night) Quiet() Night {
	n.Enabled = true
	n.DND = true
	n.Volume = 0
	n.Windows = []Window{{Days: []int{0, 1, 2, 3, 4, 5, 6}, Start: 0, Length: 24}}
	return n
}
func NightEqual(a, b Night) bool {
	near := func(x, y float64) bool { return math.Abs(x-y) < 0.001 }
	return a.Enabled == b.Enabled && a.DND == b.DND && near(a.Volume, b.Volume) &&
		near(a.Brightness, b.Brightness) && reflect.DeepEqual(a.Windows, b.Windows)
}

type Snapshot struct {
	At       time.Time      `json:"at"`
	Uptime   float64        `json:"uptime"`
	Boot     time.Time      `json:"boot"`
	Night    *Night         `json:"night,omitempty"`
	Volume   *Volume        `json:"volume,omitempty"`
	Model    string         `json:"model"`
	Firmware string         `json:"firmware"`
	Role     string         `json:"role,omitempty"`     // router: root or leaf
	Backhaul string         `json:"backhaul,omitempty"` // wired or wireless; empty for the root
	Update   string         `json:"update,omitempty"`   // router updateStatus, e.g. idle
	Raw      map[string]any `json:"-"`
}

func (c *Client) request(ctx context.Context, d config.Device, method, path string, body any, token string) ([]byte, int, error) {
	if d.IP == "" {
		return nil, 0, errors.New("device not found on the LAN")
	}
	scheme, port := "https", "8443"
	if d.Kind == "router" {
		scheme, port = "http", "80"
	}
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, errors.New("invalid local payload")
		}
		reader = bytes.NewReader(b)
	}
	// path is a literal: percent-encoding the commas causes this firmware to return {}.
	req, err := http.NewRequestWithContext(ctx, method, scheme+"://"+net.JoinHostPort(d.IP, port)+path, reader)
	if err != nil {
		return nil, 0, errors.New("invalid local URL")
	}
	if token != "" {
		req.Header.Set("cast-local-authorization-token", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, errors.New("local connection failed")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, resp.StatusCode, errors.New("local response read failed")
	}
	return b, resp.StatusCode, nil
}

// read tries without a token first, so telemetry from public endpoints does not
// depend on Google cloud; a token is fetched only after 401.
func (c *Client) read(ctx context.Context, d config.Device, path string) ([]byte, error) {
	b, code, err := c.request(ctx, d, http.MethodGet, path, nil, "")
	if err == nil && code == http.StatusUnauthorized && c.Tokens != nil {
		var token string
		if token, err = c.Tokens.LocalToken(ctx, d, true); err == nil {
			b, code, err = c.request(ctx, d, http.MethodGet, path, nil, token)
		}
	}
	if err == nil && code != http.StatusOK {
		err = fmt.Errorf("local GET HTTP %d", code)
	}
	return b, err
}
func object(m map[string]any, key string) map[string]any  { v, _ := m[key].(map[string]any); return v }
func text(m map[string]any, key string) string            { v, _ := m[key].(string); return v }
func number(m map[string]any, key string) (float64, bool) { v, ok := m[key].(float64); return v, ok }
func parseNight(raw any) (*Night, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("missing night_mode_params")
	}
	for _, k := range []string{"enabled", "do_not_disturb", "volume", "led_brightness", "windows"} {
		if _, ok := m[k]; !ok {
			return nil, errors.New("incomplete night mode settings")
		}
	}
	b, _ := json.Marshal(m)
	var n Night
	if json.Unmarshal(b, &n) != nil || n.Windows == nil {
		return nil, errors.New("invalid night mode settings")
	}
	return &n, nil
}

// Inspect performs read-only operations. Cast volume is optional for telemetry,
// but mandatory for the rebooter's saved snapshot.
func (c *Client) Inspect(ctx context.Context, d config.Device, withCast bool) (Snapshot, error) {
	path := "/api/v1/status"
	if d.Kind == "wifi_point" {
		path = "/setup/eureka_info?params=name,device_info,build_info,audio,net,wifi,settings,night_mode_params"
	}
	b, err := c.read(ctx, d, path)
	if err != nil {
		return Snapshot{}, err
	}
	var raw map[string]any
	if json.Unmarshal(b, &raw) != nil {
		return Snapshot{}, errors.New("invalid local JSON")
	}
	out := Snapshot{At: time.Now(), Raw: raw}
	var ok bool
	backhaul := func(wired bool) string {
		if wired {
			return "wired"
		}
		return "wireless"
	}
	if d.Kind == "router" {
		system, software := object(raw, "system"), object(raw, "software")
		out.Uptime, ok = number(system, "uptime")
		out.Model = text(system, "modelId")
		out.Firmware = text(software, "softwareVersion")
		out.Update = text(software, "updateStatus")
		out.Role = text(system, "groupRole")
		if lan, known := system["lan0Link"].(bool); known && out.Role == "leaf" {
			out.Backhaul = backhaul(lan)
		}
		if d.LocalID != "" && text(system, "hardwareId") != d.LocalID {
			return Snapshot{}, errors.New("router identity mismatch")
		}
	} else {
		info := object(raw, "device_info")
		out.Model = text(info, "model_name")
		out.Firmware = text(object(raw, "build_info"), "cast_build_revision")
		out.Uptime, ok = number(info, "uptime")
		if out.Model != "Nest Wifi point" || text(raw, "name") != d.CloudName {
			return Snapshot{}, errors.New("Point identity mismatch; check IP/cloud_name")
		}
		if d.LocalID != "" && text(info, "cloud_device_id") != d.LocalID {
			return Snapshot{}, errors.New("Point stable identity mismatch")
		}
		if eth, known := object(raw, "net")["ethernet_connected"].(bool); known {
			out.Backhaul = backhaul(eth)
		}
		out.Night, _ = parseNight(raw["night_mode_params"])
		if withCast {
			v, err := ReadVolume(ctx, d.IP)
			if err == nil {
				out.Volume = &v
			}
		}
	}
	if !ok || out.Uptime < 0 || math.IsInf(out.Uptime, 0) || math.IsNaN(out.Uptime) {
		return Snapshot{}, errors.New("device did not provide valid uptime")
	}
	out.Boot = out.At.Add(-time.Duration(out.Uptime * float64(time.Second)))
	return out, nil
}

// SetNight retries only after a definite 401, with a newly fetched LAT. This
// operation is idempotent. Reboot requests are never sent by the local client.
func (c *Client) SetNight(ctx context.Context, d config.Device, n Night) error {
	if d.Kind != "wifi_point" || c.Tokens == nil {
		return errors.New("night mode requires Point token provider")
	}
	token, err := c.Tokens.LocalToken(ctx, d, false)
	if err != nil {
		return err
	}
	body := struct {
		Night
		Demo bool `json:"demo_to_user"`
	}{Night: n}
	_, code, err := c.request(ctx, d, http.MethodPost, "/setup/assistant/set_night_mode_params", body, token)
	if err == nil && code == 401 {
		token, err = c.Tokens.LocalToken(ctx, d, true)
		if err != nil {
			return err
		}
		_, code, err = c.request(ctx, d, http.MethodPost, "/setup/assistant/set_night_mode_params", body, token)
	}
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("night mode POST HTTP %d", code)
	}
	got, err := c.Inspect(ctx, d, false)
	if err != nil {
		return err
	}
	if got.Night == nil || !NightEqual(*got.Night, n) {
		return errors.New("night mode read-back mismatch")
	}
	return nil
}
func (c *Client) RestoreVolume(ctx context.Context, d config.Device, v Volume) error {
	return RestoreVolume(ctx, d.IP, v)
}
