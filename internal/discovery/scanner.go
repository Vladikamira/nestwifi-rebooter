// Package discovery finds Nest Wifi devices on a bounded LAN range and
// reconciles them with the Wifi devices listed in Google HomeGraph.
package discovery

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

type Scanner struct {
	Config config.Discovery
	HTTP   *http.Client
}

func NewScanner(c config.Discovery) *Scanner {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DialContext = (&net.Dialer{Timeout: c.Timeout}).DialContext
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // LAN devices use self-signed certificates.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// Per-request deadlines come from get's context.
	return &Scanner{Config: c, HTTP: &http.Client{Transport: tr, CheckRedirect: noRedirect}}
}

// The root router takes ~1.4 s to validate a token, longer than a liveness probe.
const identityTimeout = 5 * time.Second

func object(m map[string]any, k string) map[string]any { v, _ := m[k].(map[string]any); return v }
func text(m map[string]any, k string) string           { v, _ := m[k].(string); return v }

// get returns the decoded body of a 200 response, otherwise nil.
func (s *Scanner) get(ctx context.Context, url, token string, timeout time.Duration) map[string]any {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	if token != "" {
		req.Header.Set("cast-local-authorization-token", token)
	}
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var body map[string]any
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 256<<10)).Decode(&body) != nil {
		return nil
	}
	return body
}

// Probe identifies a Nest/Google Wifi router (root or leaf) or a Nest Wifi
// Point speaker using read-only endpoints that do not require authorization.
func (s *Scanner) Probe(ctx context.Context, ip string) (config.Device, bool) {
	return s.probe(ctx, ip, s.Config.Timeout)
}

// Reprobe is Probe with a longer timeout, for addresses known to host a device.
func (s *Scanner) Reprobe(ctx context.Context, ip string) (config.Device, bool) {
	return s.probe(ctx, ip, identityTimeout)
}

func (s *Scanner) probe(ctx context.Context, ip string, timeout time.Duration) (config.Device, bool) {
	host := net.JoinHostPort(ip, "80")
	if raw := s.get(ctx, "http://"+host+"/api/v1/status", "", timeout); raw != nil {
		system := object(raw, "system")
		hw := text(system, "hardwareId")
		if model := text(system, "modelId"); (model == "MISTRAL" || model == "GALE") && hw != "" {
			return config.Device{Name: "router-" + strings.ReplaceAll(ip, ".", "-"), IP: ip, Kind: "router", LocalID: hw}, true
		}
	}
	raw := s.get(ctx, "https://"+net.JoinHostPort(ip, "8443")+"/setup/eureka_info?params=name,device_info", "", timeout)
	info := object(raw, "device_info")
	id, name := text(info, "cloud_device_id"), text(raw, "name")
	if text(info, "model_name") != "Nest Wifi point" || id == "" || name == "" {
		return config.Device{}, false
	}
	sum := sha256.Sum256([]byte(id))
	return config.Device{Name: "point-" + hex.EncodeToString(sum[:6]), IP: ip, Kind: "wifi_point", CloudName: name, LocalID: id}, true
}

// Accepts reports whether a router accepts a local token. Router tokens are
// device-specific, which makes this an identity check.
func (s *Scanner) Accepts(ctx context.Context, ip, token string) bool {
	return token != "" && s.get(ctx, "https://"+net.JoinHostPort(ip, "8443")+"/api/v1/weave-info", token, identityTimeout) != nil
}

func addresses(cidrs []string) ([]string, error) {
	seen := map[netip.Addr]bool{}
	for _, cidr := range cidrs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil || !prefix.Addr().Is4() || !prefix.Addr().IsPrivate() || prefix.Bits() < 22 {
			return nil, errors.New("discovery requires private IPv4 prefixes of /22 or smaller")
		}
		prefix = prefix.Masked()
		for ip := prefix.Addr(); prefix.Contains(ip); ip = ip.Next() {
			// Skip network and broadcast addresses.
			if prefix.Bits() < 31 && (ip == prefix.Addr() || !prefix.Contains(ip.Next())) {
				continue
			}
			if seen[ip] = true; len(seen) > 1024 {
				return nil, errors.New("discovery exceeds 1024 addresses")
			}
		}
	}
	out := make([]string, 0, len(seen))
	for ip := range seen {
		out = append(out, ip.String())
	}
	sort.Strings(out)
	return out, nil
}

func (s *Scanner) Scan(ctx context.Context) ([]config.Device, error) {
	ips, err := addresses(s.Config.CIDRs)
	if err != nil {
		return nil, err
	}
	if s.Config.Concurrency < 1 || s.Config.Concurrency > 64 || s.Config.Timeout <= 0 {
		return nil, errors.New("invalid scanner limits")
	}
	jobs := make(chan string)
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		found []config.Device
	)
	for range s.Config.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ip := range jobs {
				if d, ok := s.Probe(ctx, ip); ok {
					mu.Lock()
					found = append(found, d)
					mu.Unlock()
				}
			}
		}()
	}
feed:
	for _, ip := range ips {
		select {
		case jobs <- ip:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(found, func(i, j int) bool { return found[i].IP < found[j].IP })
	return found, nil
}
