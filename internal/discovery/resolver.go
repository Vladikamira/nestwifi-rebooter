package discovery

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

type Graph interface {
	Graph(context.Context, bool) ([]cloud.GraphDevice, error)
}
type Prober interface {
	Scan(context.Context) ([]config.Device, error)
	Accepts(ctx context.Context, ip, token string) bool
}

type Result struct {
	Devices []config.Device `json:"devices"`
	// Complete means every HomeGraph Wifi device was found exactly once on the
	// LAN and no unidentified Wifi device remained. Reboot requires it.
	Complete bool `json:"complete"`
	Expected int  `json:"expected"`
	Matched  int  `json:"matched"`
	// Missing are HomeGraph Wifi devices not found on the LAN (no IP).
	Missing []config.Device `json:"missing,omitempty"`
	At      time.Time       `json:"at"`
}

type Source interface {
	Discover(context.Context, bool) (Result, error)
}

type Resolver struct {
	mu       sync.Mutex
	interval time.Duration
	graph    Graph
	prober   Prober
	last     Result
	lastErr  error
}

func New(c config.Config, graph Graph) *Resolver {
	return &Resolver{interval: c.Discovery.Interval, graph: graph, prober: NewScanner(c.Discovery)}
}

// Canonical normalizes hex identifiers only; opaque IDs stay case-sensitive.
func Canonical(id string) string {
	v := strings.ReplaceAll(id, "-", "")
	if (len(v) == 16 || len(v) == 32) && strings.Trim(strings.ToLower(v), "0123456789abcdef") == "" {
		return strings.ToLower(v)
	}
	return id
}

func wifiKind(g cloud.GraphDevice) string {
	switch {
	case g.Type == "action.devices.types.ROUTER":
		return "router"
	case g.Model == "Nest Wifi point":
		return "wifi_point"
	}
	return ""
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

// Slug turns a Google Home device name into a metric label, e.g. "office-wifi".
func Slug(name string) string {
	return strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(name), "-"), "-")
}

// reconcile links HomeGraph Wifi devices to LAN devices by stable identity:
// Points by cloud_device_id, routers by acceptance of their own local token.
func reconcile(ctx context.Context, found []config.Device, graph []cloud.GraphDevice, accepts func(context.Context, string, string) bool) Result {
	result := Result{At: time.Now()}
	claimed := map[string]bool{}
	names := map[string]bool{}
	ok := true
	for _, g := range graph {
		kind := wifiKind(g)
		if kind == "" {
			continue
		}
		result.Expected++
		var matches []config.Device
		for _, d := range found {
			if d.Kind != kind {
				continue
			}
			if kind == "wifi_point" && g.CloudDeviceID != "" && Canonical(d.LocalID) == Canonical(g.CloudDeviceID) ||
				kind == "router" && accepts(ctx, d.IP, g.Token) {
				matches = append(matches, d)
			}
		}
		if len(matches) != 1 || g.ID == "" || claimed[matches[0].IP] {
			ok = false
			if len(matches) == 0 && g.ID != "" {
				result.Missing = append(result.Missing, config.Device{Name: Slug(g.Name), Kind: kind, CloudID: g.ID, CloudName: g.Name})
			}
			continue
		}
		d := matches[0]
		claimed[d.IP] = true
		d.CloudID = g.ID
		if kind == "router" {
			d.CloudName = g.Name
		}
		if name := Slug(g.Name); name != "" && !names[name] {
			d.Name = name
		}
		names[d.Name] = true
		result.Devices = append(result.Devices, d)
		result.Matched++
	}
	// Unidentified devices keep their telemetry but block reboot.
	for _, d := range found {
		if !claimed[d.IP] {
			ok = false
			result.Devices = append(result.Devices, d)
		}
	}
	sort.Slice(result.Devices, func(i, j int) bool { return result.Devices[i].Name < result.Devices[j].Name })
	result.Complete = ok && result.Expected > 0 && result.Matched == result.Expected
	return result
}

func (r *Resolver) Discover(ctx context.Context, force bool) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !force && !r.last.At.IsZero() && time.Since(r.last.At) < r.interval {
		return r.last, r.lastErr
	}
	found, err := r.prober.Scan(ctx)
	if err != nil {
		return r.last, err
	}
	var graph []cloud.GraphDevice
	err = errors.New("HomeGraph unavailable: inventory cannot be confirmed")
	if r.graph != nil {
		task, cancel := context.WithTimeout(ctx, 45*time.Second)
		// A rescan is infrequent and LATs change after reboots, so refresh them.
		graph, err = r.graph.Graph(task, true)
		cancel()
	}
	result := reconcile(ctx, found, graph, r.prober.Accepts)
	if err != nil {
		result.Complete = false
	}
	if ctx.Err() != nil {
		return r.last, ctx.Err()
	}
	r.last, r.lastErr = result, err
	return result, err
}
