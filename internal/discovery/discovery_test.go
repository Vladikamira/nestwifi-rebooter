package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

const router = "action.devices.types.ROUTER"

// Mirrors the verified home: one root router, two leaf routers, two Points.
func fixture() ([]config.Device, []cloud.GraphDevice) {
	found := []config.Device{
		{Name: "router-1", IP: "192.168.86.1", Kind: "router", LocalID: "HW-ROOT"},
		{Name: "router-25", IP: "192.168.86.25", Kind: "router", LocalID: "HW-OFFICE"},
		{Name: "router-94", IP: "192.168.86.94", Kind: "router", LocalID: "HW-STORAGE"},
		{Name: "point-a", IP: "192.168.86.32", Kind: "wifi_point", CloudName: "Kitchen Wifi", LocalID: "kitchen-local"},
		{Name: "point-b", IP: "192.168.86.220", Kind: "wifi_point", CloudName: "Kids room Wifi", LocalID: "kids-local"},
	}
	graph := []cloud.GraphDevice{
		{ID: "g-living", Name: "Living Room Wifi", Type: router, Token: "lat-living"},
		{ID: "g-office", Name: "Office WiFi", Type: router, Token: "lat-office"},
		{ID: "g-storage", Name: "Storage Wifi", Type: router, Token: "lat-storage"},
		{ID: "g-kitchen", Name: "Kitchen Wifi", Model: "Nest Wifi point", CloudDeviceID: "kitchen-local", Token: "lat-k"},
		{ID: "g-kids", Name: "Kids room Wifi", Model: "Nest Wifi point", CloudDeviceID: "kids-local", Token: "lat-kids"},
		{ID: "g-tv", Name: "TV", Type: "action.devices.types.TV", Token: "lat-tv"},
	}
	return found, graph
}

// Each router accepts only its own token, as observed on the real network.
var owners = map[string]string{"192.168.86.1": "lat-living", "192.168.86.25": "lat-office", "192.168.86.94": "lat-storage"}

func accepts(_ context.Context, ip, token string) bool { return owners[ip] == token }

func TestReconcileVerifiedTopology(t *testing.T) {
	found, graph := fixture()
	r := reconcile(context.Background(), found, graph, accepts)
	if !r.Complete || r.Expected != 5 || r.Matched != 5 || len(r.Devices) != 5 {
		t.Fatalf("%+v", r)
	}
	want := map[string]string{"living-room-wifi": "192.168.86.1", "office-wifi": "192.168.86.25", "storage-wifi": "192.168.86.94", "kitchen-wifi": "192.168.86.32", "kids-room-wifi": "192.168.86.220"}
	for _, d := range r.Devices {
		if want[d.Name] != d.IP || d.CloudID == "" {
			t.Fatalf("wrong mapping %+v", d)
		}
	}
	if b, _ := json.Marshal(r); strings.Contains(string(b), "lat-") {
		t.Fatal("token leaked")
	}
}

func TestMissingDeviceBlocks(t *testing.T) {
	found, graph := fixture()
	if r := reconcile(context.Background(), found[1:], graph, accepts); r.Complete || r.Matched != 4 {
		t.Fatalf("%+v", r)
	}
}

func TestUnknownLocalDeviceBlocksButKeepsTelemetry(t *testing.T) {
	found, graph := fixture()
	found = append(found, config.Device{Name: "extra", IP: "192.168.86.50", Kind: "router", LocalID: "HW-X"})
	r := reconcile(context.Background(), found, graph, accepts)
	if r.Complete || len(r.Devices) != 6 {
		t.Fatalf("%+v", r)
	}
}

func TestAmbiguousRouterTokenBlocks(t *testing.T) {
	found, graph := fixture()
	all := func(context.Context, string, string) bool { return true }
	if r := reconcile(context.Background(), found, graph, all); r.Complete {
		t.Fatal("token accepted by several routers")
	}
}

func TestPointMatchedByIdentityNotName(t *testing.T) {
	found, graph := fixture()
	graph[3].CloudDeviceID = "other"
	if r := reconcile(context.Background(), found, graph, accepts); r.Complete {
		t.Fatal("matched by name")
	}
}

func TestNoGraphMeansTelemetryOnly(t *testing.T) {
	found, _ := fixture()
	if r := reconcile(context.Background(), found, nil, accepts); r.Complete || len(r.Devices) != 5 {
		t.Fatalf("%+v", r)
	}
}

func TestCanonical(t *testing.T) {
	if Canonical("opaque-AbCd") == Canonical("opaque-abcd") || Canonical("0123456789ABCDEF") != "0123456789abcdef" {
		t.Fatal("bad normalization")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reply(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

func TestScannerReadOnlyProbes(t *testing.T) {
	s := NewScanner(config.Discovery{CIDRs: []string{"192.168.86.0/29"}, Concurrency: 2, Timeout: time.Second})
	s.HTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatal("mutating request")
		}
		switch r.URL.Hostname() + r.URL.Path {
		case "192.168.86.1/api/v1/status":
			return reply(200, `{"system":{"modelId":"MISTRAL","hardwareId":"HW","uptime":1}}`), nil
		case "192.168.86.2/setup/eureka_info":
			if strings.Contains(r.URL.RawQuery, "%2C") {
				t.Fatal("encoded commas")
			}
			return reply(200, `{"name":"Kitchen Wifi","device_info":{"model_name":"Nest Wifi point","cloud_device_id":"id"}}`), nil
		case "192.168.86.3/setup/eureka_info":
			return reply(200, `{"name":"Nest Audio","device_info":{"model_name":"Nest Audio","cloud_device_id":"id2"}}`), nil
		}
		return reply(404, `{}`), nil
	})}
	got, err := s.Scan(context.Background())
	if err != nil || len(got) != 2 || got[0].LocalID != "HW" || got[1].Kind != "wifi_point" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestAddressBounds(t *testing.T) {
	for _, cidr := range []string{"0.0.0.0/0", "192.168.0.0/16", "8.8.8.0/24", "::/0"} {
		if _, err := addresses([]string{cidr}); err == nil {
			t.Fatal("unsafe CIDR accepted:", cidr)
		}
	}
	if ips, err := addresses([]string{"192.168.86.0/30", "192.168.86.0/30"}); err != nil || len(ips) != 2 {
		t.Fatal("duplicates not removed")
	}
}

func TestScanCancellation(t *testing.T) {
	s := NewScanner(config.Discovery{CIDRs: []string{"192.168.86.0/24"}, Timeout: time.Second, Concurrency: 2})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Scan(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored")
	}
}

type fakeGraph struct {
	graph  []cloud.GraphDevice
	err    error
	forced []bool
}

func (g *fakeGraph) Graph(_ context.Context, force bool) ([]cloud.GraphDevice, error) {
	g.forced = append(g.forced, force)
	return g.graph, g.err
}

type fakeProber struct {
	found []config.Device
	slow  map[string]config.Device // answer only to the slower Reprobe
	scans int
}

func (p *fakeProber) Scan(context.Context) ([]config.Device, error) {
	p.scans++
	return append([]config.Device(nil), p.found...), nil
}
func (p *fakeProber) Reprobe(_ context.Context, ip string) (config.Device, bool) {
	d, ok := p.slow[ip]
	return d, ok
}
func (p *fakeProber) Accepts(ctx context.Context, ip, token string) bool {
	return accepts(ctx, ip, token)
}

func newResolver(g Graph, p Prober) *Resolver {
	return &Resolver{interval: time.Minute, graph: g, prober: p, known: map[string]bool{}}
}

func TestSlowPointFoundByReprobe(t *testing.T) {
	found, graph := fixture()
	p := &fakeProber{found: found}
	r := newResolver(&fakeGraph{graph: graph}, p)
	if res, _ := r.Discover(context.Background(), true); !res.Complete {
		t.Fatal("first scan incomplete")
	}
	// The kitchen Point misses the fast scan but answers a slower probe.
	kitchen := found[3]
	p.found = append(found[:3:3], found[4])
	p.slow = map[string]config.Device{kitchen.IP: kitchen}
	if res, err := r.Discover(context.Background(), true); err != nil || !res.Complete {
		t.Fatalf("known device not re-probed: %+v %v", res, err)
	}
}

func TestProblemsExplainIncompleteInventory(t *testing.T) {
	found, graph := fixture()
	r := newResolver(&fakeGraph{graph: graph}, &fakeProber{found: found[1:]})
	res, _ := r.Discover(context.Background(), true)
	if res.Complete || !slices.Contains(res.Problems, "Living Room Wifi: not found on the LAN") {
		t.Fatalf("%+v", res.Problems)
	}
	g := &fakeGraph{err: errors.New("GetHomeGraph failed: Unavailable: x")}
	res, _ = newResolver(g, &fakeProber{found: found}).Discover(context.Background(), true)
	if !slices.ContainsFunc(res.Problems, func(s string) bool { return strings.Contains(s, "Unavailable") }) {
		t.Fatalf("cloud reason missing: %+v", res.Problems)
	}
}

func TestOnlyForcedDiscoveryForcesHomeGraph(t *testing.T) {
	found, graph := fixture()
	g := &fakeGraph{graph: graph}
	r := newResolver(g, &fakeProber{found: found})
	r.interval = 0 // rescan on every call
	_, _ = r.Discover(context.Background(), false)
	_, _ = r.Discover(context.Background(), true)
	if !slices.Equal(g.forced, []bool{false, true}) {
		t.Fatalf("HomeGraph force flags %v", g.forced)
	}
}

func TestResolverCachingAndCloudFailure(t *testing.T) {
	found, graph := fixture()
	g, p := &fakeGraph{graph: graph}, &fakeProber{found: found}
	r := newResolver(g, p)
	for range 2 {
		if res, err := r.Discover(context.Background(), false); err != nil || !res.Complete {
			t.Fatal("resolve failed", err)
		}
	}
	if p.scans != 1 {
		t.Fatal("cache ignored")
	}
	g.err = errors.New("offline")
	if res, err := r.Discover(context.Background(), true); err == nil || res.Complete || len(res.Devices) != 5 || p.scans != 2 {
		t.Fatalf("cloud failure handling: %+v %v", res, err)
	}
}
