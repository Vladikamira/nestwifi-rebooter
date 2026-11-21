package discovery

import (
	"context"
	"errors"
	"fmt"
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
	Reprobe(ctx context.Context, ip string) (config.Device, bool)
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
	// Problems explains why the inventory is not complete (no secrets).
	Problems []string  `json:"problems,omitempty"`
	At       time.Time `json:"at"`
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
	known    map[string]bool // IPs where a Wifi device answered before
}

func New(c config.Config, graph Graph) *Resolver {
	return &Resolver{interval: c.Discovery.Interval, graph: graph, prober: NewScanner(c.Discovery), known: map[string]bool{}}
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
			switch {
			case len(matches) == 0 && g.ID != "":
				result.Missing = append(result.Missing, config.Device{Name: Slug(g.Name), Kind: kind, CloudID: g.ID, CloudName: g.Name})
				result.Problems = append(result.Problems, g.Name+": not found on the LAN")
			case len(matches) > 1:
				result.Problems = append(result.Problems, fmt.Sprintf("%s: matches %d LAN devices", g.Name, len(matches)))
			default:
				result.Problems = append(result.Problems, g.Name+": ambiguous identity")
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
			result.Problems = append(result.Problems, fmt.Sprintf("%s %s: not identified in HomeGraph", d.Kind, d.IP))
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
	found = r.reprobe(ctx, found)
	var graph []cloud.GraphDevice
	err = errors.New("HomeGraph unavailable: inventory cannot be confirmed")
	if r.graph != nil {
		task, cancel := context.WithTimeout(ctx, 45*time.Second)
		// force: fresh LATs (they change after reboots) before changing devices;
		// telemetry may use the cached graph.
		graph, err = r.graph.Graph(task, force)
		cancel()
	}
	result := reconcile(ctx, found, graph, r.prober.Accepts)
	if err != nil {
		result.Complete = false
		result.Problems = append(result.Problems, err.Error())
	}
	if ctx.Err() != nil {
		return r.last, ctx.Err()
	}
	for _, d := range found {
		r.known[d.IP] = true
	}
	r.last, r.lastErr = result, err
	return result, err
}

// reprobe retries, with a longer timeout, addresses where a device was seen
// before but did not answer the fast scan: Points sometimes answer slowly.
func (r *Resolver) reprobe(ctx context.Context, found []config.Device) []config.Device {
	seen := map[string]bool{}
	for _, d := range found {
		seen[d.IP] = true
	}
	for ip := range r.known {
		if seen[ip] || ctx.Err() != nil {
			continue
		}
		if d, ok := r.prober.Reprobe(ctx, ip); ok {
			found = append(found, d)
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].IP < found[j].IP })
	return found
}
