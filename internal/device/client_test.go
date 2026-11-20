package device

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type tokens struct{ forced int }

func (t *tokens) LocalToken(_ context.Context, _ config.Device, force bool) (string, error) {
	if force {
		t.forced++
		return "fresh", nil
	}
	return "old", nil
}
func reply(code int, b string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(b)), Header: http.Header{}}
}
func TestSetNightRefreshesOn401AndVerifies(t *testing.T) {
	tokens := &tokens{}
	c := New(tokens)
	posts := 0
	n := Night{Brightness: 1, Volume: 0, Enabled: true, DND: true, Windows: []Window{{Days: []int{0, 1, 2, 3, 4, 5, 6}, Start: 0, Length: 24}}}
	c.HTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == "POST" {
			posts++
			if r.Header.Get("cast-local-authorization-token") == "old" {
				return reply(401, ""), nil
			}
			if r.Header.Get("cast-local-authorization-token") != "fresh" {
				t.Fatal("missing refreshed token")
			}
			return reply(200, "{}"), nil
		}
		if strings.Contains(r.URL.RawQuery, "%2C") {
			t.Fatal("commas were encoded")
		}
		b, _ := json.Marshal(map[string]any{"name": "Kitchen Wifi", "device_info": map[string]any{"model_name": "Nest Wifi point", "uptime": 123}, "night_mode_params": n})
		return reply(200, string(b)), nil
	})}
	if err := c.SetNight(context.Background(), config.Device{IP: "192.0.2.2", Kind: "wifi_point", CloudName: "Kitchen Wifi"}, n); err != nil {
		t.Fatal(err)
	}
	if posts != 2 || tokens.forced != 1 {
		t.Fatal("refresh/retry policy incorrect")
	}
}
func TestSetNightDoesNotRetryServerError(t *testing.T) {
	c := New(&tokens{})
	calls := 0
	c.HTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { calls++; return reply(500, "private details"), nil })}
	if c.SetNight(context.Background(), config.Device{IP: "192.0.2.2", Kind: "wifi_point"}, Night{}) == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatal("retried server error")
	}
}
func TestRouterRoleBackhaulAndIdentity(t *testing.T) {
	c := New(nil)
	c.HTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return reply(200, `{"system":{"uptime":5,"modelId":"MISTRAL","groupRole":"leaf","lan0Link":false,"hardwareId":"HW-1"},"software":{"updateStatus":"idle"}}`), nil
	})}
	d := config.Device{IP: "192.0.2.25", Kind: "router", LocalID: "HW-1"}
	s, err := c.Inspect(context.Background(), d, false)
	if err != nil || s.Role != "leaf" || s.Backhaul != "wireless" || s.Update != "idle" {
		t.Fatalf("%+v %v", s, err)
	}
	d.LocalID = "HW-OTHER"
	if _, err := c.Inspect(context.Background(), d, false); err == nil {
		t.Fatal("different router accepted after IP change")
	}
}

func TestTelemetryDoesNotNeedCloudForPublicEndpoints(t *testing.T) {
	c := New(nil)
	c.HTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return reply(200, `{"system":{"uptime":42,"modelId":"MISTRAL"},"software":{"softwareVersion":"test"}}`), nil
	})}
	s, err := c.Inspect(context.Background(), config.Device{IP: "192.0.2.1", Kind: "router"}, false)
	if err != nil || s.Uptime != 42 || s.Model != "MISTRAL" {
		t.Fatal("router inspect failed")
	}
}
func TestMissingUptimeRejected(t *testing.T) {
	c := New(nil)
	c.HTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return reply(200, `{}`), nil })}
	if _, err := c.Inspect(context.Background(), config.Device{IP: "192.0.2.1", Kind: "router"}, false); err == nil {
		t.Fatal("missing uptime accepted")
	}
}
func TestIncompleteNightRejected(t *testing.T) {
	if _, err := parseNight(map[string]any{"enabled": true}); err == nil {
		t.Fatal("missing original state accepted")
	}
}
func TestQuietPreservesBrightness(t *testing.T) {
	original := Night{Brightness: 0.42, Volume: 0.8, Windows: []Window{{Days: []int{1}, Start: 22, Length: 8}}}
	quiet := original.Quiet()
	if !quiet.Enabled || !quiet.DND || quiet.Volume != 0 || quiet.Brightness != 0.42 || len(quiet.Windows[0].Days) != 7 || quiet.Windows[0].Length != 24 {
		t.Fatal("wrong quiet mode")
	}
	if original.Enabled || original.Volume != 0.8 {
		t.Fatal("modified original state")
	}
}
func TestCastFrameRoundtrip(t *testing.T) {
	ns, p, err := castPayload(castFrame(receiverNS, []byte(`{"type":"GET_STATUS"}`)))
	if err != nil || ns != receiverNS || string(p) != `{"type":"GET_STATUS"}` {
		t.Fatal("Cast framing mismatch")
	}
}
