// Package cloud implements the unofficial Google Home authentication and Foyer APIs.
// Secrets and raw server responses must never be included in errors or logs.
package cloud

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

const foyer = "https://googlehomefoyer-pa.googleapis.com/v2"

type GraphDevice struct {
	Home, ID, Name, CloudDeviceID, Model, Type string
	Token                                      string `json:"-"`
}
type Manager struct {
	mu                    sync.Mutex
	statsMu               sync.RWMutex
	home                  string
	authHTTP, apiHTTP     *http.Client
	access                string
	expires               time.Time
	graph                 []GraphDevice
	graphExpires          time.Time
	masterHash            [32]byte
	lastAccess, lastGraph time.Time
}

func New(home string) *Manager {
	// Android auth must use HTTP/1.1 without ALPN. A clone of DefaultTransport
	// keeps offering h2 and breaks, so build a fresh HTTP/1-only transport.
	h1 := new(http.Protocols)
	h1.SetHTTP1(true)
	authTransport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		Protocols:           h1,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: 10 * time.Second,
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Manager{home: home, authHTTP: &http.Client{
		Transport:     authTransport,
		Timeout:       25 * time.Second,
		CheckRedirect: noRedirect},
		apiHTTP: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: noRedirect,
		},
	}
}

func readMaster() (string, error) {
	if path := os.Getenv("GOOGLE_MASTER_TOKEN_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", errors.New("cannot read master token file")
		}
		return strings.TrimSpace(string(b)), nil
	}
	return strings.TrimSpace(os.Getenv("GOOGLE_MASTER_TOKEN")), nil
}

func (m *Manager) accessLocked(ctx context.Context, force bool) (string, error) {
	master, err := readMaster()
	if err != nil {
		return "", err
	}
	email := strings.TrimSpace(os.Getenv("GOOGLE_EMAIL"))
	if master == "" || email == "" {
		return "", errors.New("GOOGLE_EMAIL and master token input are required")
	}
	hash := sha256.Sum256([]byte(email + "\x00" + master))
	if !force && hash == m.masterHash && m.access != "" && time.Now().Before(m.expires) {
		return m.access, nil
	}
	android := os.Getenv("GOOGLE_ANDROID_ID")
	if android == "" {
		android = "0123456789abcdef"
	}
	// Same parameters as gpsoauth.perform_oauth for the Google Home app.
	form := url.Values{
		"accountType":                  {"HOSTED_OR_GOOGLE"},
		"Email":                        {email},
		"has_permission":               {"1"},
		"EncryptedPasswd":              {master},
		"service":                      {"oauth2:https://www.google.com/accounts/OAuthLogin"},
		"source":                       {"android"},
		"androidId":                    {android},
		"app":                          {"com.google.android.apps.chromecast.app"},
		"client_sig":                   {"24bb24c05e47e0aefa68a58a766179d9b613a600"},
		"device_country":               {"us"},
		"operatorCountry":              {"us"},
		"lang":                         {"en"},
		"sdk_version":                  {"17"},
		"google_play_services_version": {"240913000"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://android.clients.google.com/auth", strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("cannot create auth request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "GoogleAuth/1.4")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := m.authHTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("Google authentication connection failed: %w", netCause(err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", errors.New("cannot read authentication response")
	}

	values := map[string]string{}
	for _, line := range strings.Split(string(body), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok {
			values[k] = v
		}
	}

	if resp.StatusCode != 200 || values["Auth"] == "" {
		return "", errors.New("Google authentication rejected master token; renew it externally")
	}

	ttl := 45 * time.Minute
	if n, e := strconv.ParseInt(values["Expiry"], 10, 64); e == nil {
		d := time.Until(time.Unix(n, 0)) - 2*time.Minute
		if d > 0 && d < ttl {
			ttl = d
		}
	}
	m.access = values["Auth"]
	m.expires = time.Now().Add(ttl)
	m.masterHash = hash
	m.statsMu.Lock()
	m.lastAccess = time.Now()
	m.statsMu.Unlock()
	return m.access, nil
}

func (m *Manager) Access(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.accessLocked(ctx, false)
}

// graphRequest returns raw GetHomeGraph protobuf bytes without generated code:
// the request is empty, and every reply field is kept as an unknown field of
// emptypb.Empty. A custom codec would send application/grpc+proto, which
// Google rejects with 404.
func graphRequest(ctx context.Context, token string) ([]byte, error) {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	conn, err := grpc.NewClient("googlehomefoyer-pa.googleapis.com:443", grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, errors.New("cannot create HomeGraph connection")
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	reply := new(emptypb.Empty)
	err = conn.Invoke(ctx, "/google.internal.home.foyer.v1.StructuresService/GetHomeGraph",
		new(emptypb.Empty), reply, grpc.MaxCallRecvMsgSize(8<<20))
	return reply.ProtoReflect().GetUnknown(), err
}

// bytesFields ignores unknown wire fields. Numbers match google/internal/home/foyer/v1.proto.
func bytesFields(data []byte, number protowire.Number) ([][]byte, error) {
	var out [][]byte
	for len(data) > 0 {
		n, t, k := protowire.ConsumeTag(data)
		if k < 0 {
			return nil, errors.New("invalid HomeGraph protobuf")
		}
		data = data[k:]
		if t == protowire.BytesType {
			v, k := protowire.ConsumeBytes(data)
			if k < 0 {
				return nil, errors.New("invalid HomeGraph protobuf")
			}
			if n == number {
				out = append(out, v)
			}
			data = data[k:]
		} else {
			k := protowire.ConsumeFieldValue(n, t, data)
			if k < 0 {
				return nil, errors.New("invalid HomeGraph protobuf")
			}
			data = data[k:]
		}
	}
	return out, nil
}

func one(data []byte, n protowire.Number) string {
	v, _ := bytesFields(data, n)
	if len(v) == 0 {
		return ""
	}
	return string(v[len(v)-1])
}

func parseGraph(data []byte) ([]GraphDevice, error) {
	homes, err := bytesFields(data, 2)
	if err != nil {
		return nil, err
	}
	var out []GraphDevice
	for _, home := range homes {
		devices, err := bytesFields(home, 7)
		if err != nil {
			return nil, err
		}
		for _, d := range devices {
			info := []byte(one(d, 1))
			out = append(out, GraphDevice{
				Home:          one(home, 1),
				ID:            one(info, 1),
				CloudDeviceID: one([]byte(one(info, 2)), 2), // equals the device's local cloud_device_id
				Name:          one(d, 4),
				Type:          one(d, 6),
				Model:         one([]byte(one(d, 17)), 2),
				Token:         one(d, 28),
			})
		}
	}
	return out, nil
}

func (m *Manager) graphLocked(ctx context.Context, force bool) error {
	if force || time.Now().After(m.graphExpires) || len(m.graph) == 0 {
		token, err := m.accessLocked(ctx, false)
		if err != nil {
			return err
		}
		data, err := graphRequest(ctx, token)
		if status.Code(err) == codes.Unauthenticated {
			token, err = m.accessLocked(ctx, true)
			if err == nil {
				data, err = graphRequest(ctx, token)
			}
		}
		if err != nil {
			// gRPC code and message carry no credentials (e.g. Unavailable, ResourceExhausted).
			s := status.Convert(err)
			return fmt.Errorf("GetHomeGraph failed: %s: %.120s", s.Code(), s.Message())
		}
		graph, err := parseGraph(data)
		if err != nil {
			return err
		}
		m.graph = graph
		m.graphExpires = time.Now().Add(10 * time.Minute)
		m.statsMu.Lock()
		m.lastGraph = time.Now()
		m.statsMu.Unlock()
	}
	return nil
}

// netCause strips the request (method, URL) from a transport error and keeps
// the network reason, e.g. "i/o timeout" or "no such host". Neither the URL
// nor the reason contains credentials, but the URL is not useful in logs.
func netCause(err error) error {
	var u *url.Error
	if errors.As(err, &u) {
		return u.Err
	}
	return err
}

// Graph is for internal inventory reconciliation. Its tokens must not be logged.
// With force=false a failed refresh falls back to the cached graph (if any),
// which is fine for telemetry; callers that change devices pass force=true.
func (m *Manager) Graph(ctx context.Context, force bool) ([]GraphDevice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.graphLocked(ctx, force); err != nil {
		if force || len(m.graph) == 0 {
			return nil, err
		}
		slog.Warn("HomeGraph refresh failed; using cached inventory", "error", err)
	}
	var out []GraphDevice
	for _, d := range m.graph {
		if m.home == "" || d.Home == m.home {
			out = append(out, d)
		}
	}
	return out, nil
}

func (m *Manager) LocalToken(ctx context.Context, d config.Device, force bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.graphLocked(ctx, force); err != nil {
		return "", err
	}
	var matches []GraphDevice
	for _, v := range m.graph {
		if m.home != "" && v.Home != m.home {
			continue
		}
		if (d.CloudID != "" && v.ID == d.CloudID) || (d.CloudID == "" && v.Name == d.CloudName) {
			matches = append(matches, v)
		}
	}
	if len(matches) != 1 || matches[0].Token == "" {
		return "", errors.New("HomeGraph device missing, ambiguous, or has no LAT; configure home_id/cloud_id")
	}
	return matches[0].Token, nil
}

func (m *Manager) Refresh(ctx context.Context, d config.Device) error {
	_, err := m.LocalToken(ctx, d, true)
	return err
}
func (m *Manager) RefreshTimes() (time.Time, time.Time) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	return m.lastAccess, m.lastGraph
}

type Group struct {
	ID           string        `json:"id"`
	AccessPoints []AccessPoint `json:"accessPoints"`
}

// AccessPoint deliberately omits localAuthorizationToken and serialNumber
// present in the API response, so they cannot leak through this type.
type AccessPoint struct {
	ID       string `json:"id"`
	Settings struct {
		Other struct {
			Name string `json:"apName"`
		} `json:"accessPointOtherSettings"`
		Lighting struct {
			Intensity int `json:"intensity"` // 0..100
		} `json:"lightingSettings"`
	} `json:"accessPointSettings"`
	Properties struct {
		IP       string    `json:"ipAddress"`
		Root     bool      `json:"isGroupRoot"`
		Hardware string    `json:"hardwareType"`
		Firmware string    `json:"firmwareVersion"`
		LastSeen time.Time `json:"lastSeenTime"`
	} `json:"accessPointProperties"`
}

func (a AccessPoint) Name() string { return a.Settings.Other.Name }

// rest sends one request; payload is JSON-encoded (POST defaults to {}).
func (m *Manager) rest(ctx context.Context, method, path string, payload ...any) ([]byte, int, error) {
	token, err := m.Access(ctx)
	if err != nil {
		return nil, 0, err
	}
	var body io.Reader
	switch {
	case len(payload) > 0:
		b, err := json.Marshal(payload[0])
		if err != nil {
			return nil, 0, errors.New("invalid cloud payload")
		}
		body = bytes.NewReader(b)
	case method == http.MethodPost:
		body = strings.NewReader("{}")
	}
	req, err := http.NewRequestWithContext(ctx, method, foyer+path, body)
	if err != nil {
		return nil, 0, errors.New("invalid cloud request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.apiHTTP.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("cloud connection failed (command acceptance may be unknown): %w", netCause(err))
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, errors.New("cannot read cloud response")
	}
	return b, resp.StatusCode, nil
}

// get decodes a GET response, re-minting the access token once on 401.
func (m *Manager) get(ctx context.Context, path string, out any) error {
	b, code, err := m.rest(ctx, http.MethodGet, path)
	if err == nil && code == http.StatusUnauthorized {
		m.mu.Lock()
		_, err = m.accessLocked(ctx, true)
		m.mu.Unlock()
		if err == nil {
			b, code, err = m.rest(ctx, http.MethodGet, path)
		}
	}
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("cloud GET %s HTTP %d", strings.SplitN(path, "?", 2)[0], code)
	}
	if json.Unmarshal(b, out) != nil {
		return errors.New("invalid cloud response")
	}
	return nil
}

func (m *Manager) Groups(ctx context.Context) ([]Group, error) {
	var out struct {
		Groups []Group `json:"groups"`
	}
	err := m.get(ctx, "/groups?prettyPrint=false", &out)
	return out.Groups, err
}

// Group returns the configured group, or the only one if id is empty.
func (m *Manager) Group(ctx context.Context, id string) (Group, error) {
	groups, err := m.Groups(ctx)
	if err != nil {
		return Group{}, err
	}
	for _, g := range groups {
		if g.ID == id || id == "" && len(groups) == 1 {
			return g, nil
		}
	}
	return Group{}, errors.New("Wifi group not found or ambiguous; set rebooter.group_id")
}

// CheckGroup requires the local inventory to be exactly the group's APs,
// compared by the LAN IP each AP reports to the cloud.
func (m *Manager) CheckGroup(ctx context.Context, id string, ips []string) error {
	if id == "" {
		return errors.New("group_id is required")
	}
	g, err := m.Group(ctx, id)
	if err != nil {
		return err
	}
	want := make([]string, 0, len(g.AccessPoints))
	for _, ap := range g.AccessPoints {
		want = append(want, ap.Properties.IP)
	}
	slices.Sort(want)
	got := slices.Sorted(slices.Values(ips))
	if !slices.Equal(want, got) {
		return errors.New("inventory differs from the cloud group's access points; refusing reboot")
	}
	return nil
}

// Telemetry is an aggregate, privacy-preserving view of cloud Wifi metrics:
// no client names, addresses or identifiers.
type Telemetry struct {
	Group     Group
	WANOnline bool
	APOnline  map[string]bool      // by AP ID
	Mesh      map[string]MeshSpeed // by AP ID; the root has none
	TxBps     float64
	RxBps     float64
	Clients   map[ClientKey]int
	Known     int
	Connected int
}
type MeshSpeed struct {
	Rating string  // e.g. EXCELLENT, GOOD, OK, WEAK
	Score  float64 // numericSpeedRating
}
type ClientKey struct{ APID, Connection, Band string }

func (m *Manager) Telemetry(ctx context.Context, groupID string) (Telemetry, error) {
	g, err := m.Group(ctx, groupID)
	if err != nil {
		return Telemetry{}, err
	}
	base := "/groups/" + url.PathEscape(g.ID)
	var status struct {
		WAN string `json:"wanConnectionStatus"`
		APs []struct {
			ID    string `json:"apId"`
			State string `json:"apState"`
		} `json:"apStatuses"`
	}
	var realtime struct {
		Traffic struct {
			Tx string `json:"transmitSpeedBps"`
			Rx string `json:"receiveSpeedBps"`
		} `json:"groupTraffic"`
		Stations []struct {
			Station struct {
				APID       string `json:"apId"`
				Connection string `json:"connectionType"`
				Band       string `json:"wirelessBand"`
			} `json:"station"`
		} `json:"stationMetrics"`
		Mesh []struct {
			APID  string `json:"apId"`
			Speed struct {
				Rating string `json:"speedRating"`
				Score  string `json:"numericSpeedRating"`
			} `json:"speed"`
		} `json:"meshMetrics"`
	}
	var stations struct {
		Stations []struct {
			Connected bool `json:"connected"`
		} `json:"stations"`
	}
	for path, out := range map[string]any{"/status": &status, "/realtimeMetrics": &realtime, "/stations": &stations} {
		if err := m.get(ctx, base+path+"?prettyPrint=false", out); err != nil {
			return Telemetry{}, err
		}
	}
	t := Telemetry{Group: g, WANOnline: status.WAN == "WAN_ONLINE", APOnline: map[string]bool{},
		Mesh: map[string]MeshSpeed{}, Clients: map[ClientKey]int{}, Known: len(stations.Stations)}
	for _, s := range status.APs {
		t.APOnline[s.ID] = s.State == "AP_ONLINE"
	}
	for _, x := range realtime.Mesh {
		score, _ := strconv.ParseFloat(x.Speed.Score, 64)
		t.Mesh[x.APID] = MeshSpeed{Rating: x.Speed.Rating, Score: score}
	}
	for _, s := range realtime.Stations {
		t.Clients[ClientKey{s.Station.APID, s.Station.Connection, s.Station.Band}]++
	}
	for _, s := range stations.Stations {
		if s.Connected {
			t.Connected++
		}
	}
	t.TxBps, _ = strconv.ParseFloat(realtime.Traffic.Tx, 64)
	t.RxBps, _ = strconv.ParseFloat(realtime.Traffic.Rx, 64)
	return t, nil
}

// command sends one mutating request and returns the cloud operation ID.
// It is never retried: even a transport error may mean Google accepted it.
func (m *Manager) command(ctx context.Context, method, path string, payload ...any) (string, error) {
	name := strings.SplitN(path, "?", 2)[0]
	b, code, err := m.rest(ctx, method, path, payload...)
	if err != nil {
		return "", err
	}
	if code != http.StatusOK {
		return "", fmt.Errorf("cloud %s %s HTTP %d; not retried", method, name, code)
	}
	var result struct {
		Operation struct {
			ID    string `json:"operationId"`
			State string `json:"operationState"`
		} `json:"operation"`
	}
	if json.Unmarshal(b, &result) != nil {
		return "", fmt.Errorf("ambiguous response to %s; not retried", name)
	}
	switch result.Operation.State {
	case "CREATED", "IN_PROGRESS", "RUNNING", "DONE":
		return result.Operation.ID, nil
	}
	return "", fmt.Errorf("unrecognized operation state for %s; not retried", name)
}

// Reboot restarts the whole Wifi network.
func (m *Manager) Reboot(ctx context.Context, groupID string) error {
	_, err := m.command(ctx, http.MethodPost, "/groups/"+url.PathEscape(groupID)+"/reboot?prettyPrint=false")
	return err
}

// RebootAP restarts one access point.
func (m *Manager) RebootAP(ctx context.Context, apID string) error {
	_, err := m.command(ctx, http.MethodPost, "/accesspoints/"+url.PathEscape(apID)+"/reboot?prettyPrint=false")
	return err
}

// AccessPointIP returns the LAN IP the cloud reports for an AP of the group.
func (m *Manager) AccessPointIP(ctx context.Context, groupID, apID string) (string, error) {
	g, err := m.Group(ctx, groupID)
	if err != nil {
		return "", err
	}
	for _, ap := range g.AccessPoints {
		if ap.ID == apID && ap.Properties.IP != "" {
			return ap.Properties.IP, nil
		}
	}
	return "", errors.New("access point not in the group")
}

// SetLight sets the status light brightness (0..100, 0 = off) of one AP.
func (m *Manager) SetLight(ctx context.Context, apID string, intensity int) error {
	if intensity < 0 || intensity > 100 {
		return errors.New("light intensity must be 0..100")
	}
	_, err := m.command(ctx, http.MethodPut, "/accesspoints/"+url.PathEscape(apID)+"/lighting?prettyPrint=false",
		map[string]any{"automatic": false, "intensity": intensity})
	return err
}

type SpeedTest struct {
	At          time.Time
	DownloadBps float64
	UploadBps   float64
}

// RunSpeedTest starts a WAN speed test, waits for it (~30 s) and returns the result.
func (m *Manager) RunSpeedTest(ctx context.Context, groupID string) (SpeedTest, error) {
	base := "/groups/" + url.PathEscape(groupID)
	op, err := m.command(ctx, http.MethodPost, base+"/wanSpeedTest?prettyPrint=false")
	if err != nil {
		return SpeedTest{}, err
	}
	for state := ""; state != "DONE"; {
		select {
		case <-ctx.Done():
			return SpeedTest{}, ctx.Err()
		case <-time.After(5 * time.Second):
		}
		var o struct {
			State string `json:"operationState"`
		}
		if err := m.get(ctx, "/operations/"+url.PathEscape(op)+"?prettyPrint=false", &o); err != nil {
			return SpeedTest{}, err
		}
		if state = o.State; state != "DONE" && state != "CREATED" && state != "IN_PROGRESS" {
			return SpeedTest{}, fmt.Errorf("speed test %s", state)
		}
	}
	var res struct {
		Results []struct {
			At   time.Time `json:"timestamp"`
			Up   string    `json:"transmitWanSpeedBps"`
			Down string    `json:"receiveWanSpeedBps"`
		} `json:"speedTestResults"`
	}
	if err := m.get(ctx, base+"/speedTestResults?prettyPrint=false&maxResultCount=1", &res); err != nil {
		return SpeedTest{}, err
	}
	if len(res.Results) == 0 {
		return SpeedTest{}, errors.New("speed test finished without a result")
	}
	r := res.Results[0]
	s := SpeedTest{At: r.At}
	s.DownloadBps, _ = strconv.ParseFloat(r.Down, 64)
	s.UploadBps, _ = strconv.ParseFloat(r.Up, 64)
	return s, nil
}

// Client is a Wifi client (station) by its cloud ID; used only for clients
// explicitly listed in the configuration.
type Client struct {
	ID        string `json:"id"`
	Name      string `json:"friendlyName"`
	Connected bool   `json:"connected"`
	Paused    bool   `json:"-"`
}

// Clients returns all known stations with their pause state.
func (m *Manager) Clients(ctx context.Context, groupID string) ([]Client, error) {
	base := "/groups/" + url.PathEscape(groupID)
	var st struct {
		Stations []Client `json:"stations"`
	}
	if err := m.get(ctx, base+"/stations?prettyPrint=false", &st); err != nil {
		return nil, err
	}
	var g struct { // GET /groups/{id} wraps the group: {"group": {...}}
		Group struct {
			Settings struct {
				FamilyHub struct {
					Policies []struct {
						StationID string `json:"stationId"`
						Blocking  struct {
							Expiry time.Time `json:"expiryTimestamp"`
						} `json:"blockingPolicy"`
					} `json:"stationPolicies"`
				} `json:"familyHubSettings"`
			} `json:"groupSettings"`
		} `json:"group"`
	}
	if err := m.get(ctx, base+"?prettyPrint=false", &g); err != nil {
		return nil, err
	}
	paused := map[string]bool{}
	for _, p := range g.Group.Settings.FamilyHub.Policies {
		// Ended pauses keep a past expiry; an epoch expiry means indefinitely.
		e := p.Blocking.Expiry
		paused[p.StationID] = e.Unix() <= 0 || e.After(time.Now())
	}
	for i := range st.Stations {
		st.Stations[i].Paused = paused[st.Stations[i].ID]
	}
	return st.Stations, nil
}

// SetPaused blocks or restores internet access for one client.
func (m *Manager) SetPaused(ctx context.Context, groupID, stationID string, paused bool) error {
	_, err := m.command(ctx, http.MethodPut, "/groups/"+url.PathEscape(groupID)+"/stationBlocking?prettyPrint=false",
		map[string]string{"stationId": stationID, "blocked": strconv.FormatBool(paused)})
	return err
}

// Prioritize gives one client priority for 1..6 hours; ClearPriority ends it.
func (m *Manager) Prioritize(ctx context.Context, groupID, stationID string, hours int) error {
	if hours < 1 || hours > 6 {
		return errors.New("priority duration must be 1..6 hours")
	}
	end := time.Now().Add(time.Duration(hours) * time.Hour).UTC().Format(time.RFC3339)
	_, err := m.command(ctx, http.MethodPut, "/groups/"+url.PathEscape(groupID)+"/prioritizedStation?prettyPrint=false",
		map[string]string{"stationId": stationID, "prioritizationEndTime": end})
	return err
}

func (m *Manager) ClearPriority(ctx context.Context, groupID string) error {
	_, err := m.command(ctx, http.MethodDelete, "/groups/"+url.PathEscape(groupID)+"/prioritizedStation?prettyPrint=false")
	return err
}
