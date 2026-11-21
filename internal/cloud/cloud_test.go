package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/emptypb"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}
func authEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GOOGLE_MASTER_TOKEN_FILE", "")
	t.Setenv("GOOGLE_EMAIL", "test@example.invalid")
	t.Setenv("GOOGLE_MASTER_TOKEN", "secret-master")
}
func TestAuthExchangeAndCache(t *testing.T) {
	authEnv(t)
	m := New("")
	calls := 0
	m.authHTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "android.clients.google.com" {
			t.Fatal("wrong auth endpoint")
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("EncryptedPasswd") != "secret-master" || r.Form.Get("app") != "com.google.android.apps.chromecast.app" || r.Form.Get("service") != "oauth2:https://www.google.com/accounts/OAuthLogin" {
			t.Fatal("incorrect auth form")
		}
		return response(200, "Auth=secret-access\nExpiry=9999999999\n"), nil
	})}
	for i := 0; i < 2; i++ {
		token, err := m.Access(context.Background())
		if err != nil || token != "secret-access" {
			t.Fatal("auth failed")
		}
	}
	if calls != 1 {
		t.Fatal("access token not cached")
	}
}
func TestFileMasterRotation(t *testing.T) {
	authEnv(t)
	p := filepath.Join(t.TempDir(), "master")
	if err := os.WriteFile(p, []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_MASTER_TOKEN_FILE", p)
	m := New("")
	calls := 0
	m.authHTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) { calls++; return response(200, "Auth=access\n"), nil })}
	if _, err := m.Access(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Access(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("rotated master not detected")
	}
}
func TestAuthErrorsDoNotLeakSecrets(t *testing.T) {
	authEnv(t)
	m := New("")
	m.authHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return response(403, "Error=secret-master secret-access"), nil
	})}
	_, err := m.Access(context.Background())
	if err == nil || strings.Contains(err.Error(), "secret-") {
		t.Fatal("unsafe auth error")
	}
}
func wire(n protowire.Number, b []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), b)
}
func TestHomeGraphFieldsAndUnknownFields(t *testing.T) {
	d := append(wire(1, wire(1, []byte("stable-id"))), wire(4, []byte("Kitchen Wifi"))...)
	d = append(d, wire(28, []byte("local-secret"))...)
	d = append(d, protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 123)...)
	h := append(wire(1, []byte("home")), wire(7, d)...)
	graph, err := parseGraph(wire(2, h))
	if err != nil || len(graph) != 1 {
		t.Fatal("parse failed")
	}
	g := graph[0]
	if g.ID != "stable-id" || g.Home != "home" || g.Name != "Kitchen Wifi" || g.Token != "local-secret" {
		t.Fatal("wrong protobuf mapping")
	}
}

func TestHomeGraphInnerCloudDeviceID(t *testing.T) {
	info := append(wire(1, []byte("graph-id")), wire(2, wire(2, []byte("local-cloud-device-id")))...)
	d := append(wire(1, info), wire(4, []byte("Kitchen Wifi"))...)
	graph, err := parseGraph(wire(2, wire(7, d)))
	if err != nil || len(graph) != 1 || graph[0].CloudDeviceID != "local-cloud-device-id" || graph[0].ID != "graph-id" {
		t.Fatal("nested cloud_device_id mapping failed")
	}
}
func TestGraphAmbiguousNameRejectedAndIDAllowed(t *testing.T) {
	m := New("home")
	m.graphExpires = time.Now().Add(time.Hour)
	m.graph = []GraphDevice{{Home: "home", ID: "one", Name: "Kitchen Wifi", Token: "a"}, {Home: "home", ID: "two", Name: "Kitchen Wifi", Token: "b"}}
	d := config.Device{CloudName: "Kitchen Wifi"}
	if _, err := m.LocalToken(context.Background(), d, false); err == nil {
		t.Fatal("ambiguous name accepted")
	}
	d.CloudID = "two"
	v, err := m.LocalToken(context.Background(), d, false)
	if err != nil || v != "b" {
		t.Fatal("stable ID selection failed")
	}
}
func TestMalformedGraphRejected(t *testing.T) {
	if _, err := parseGraph([]byte{0x12, 0xff}); err == nil {
		t.Fatal("malformed graph accepted")
	}
}

// GetHomeGraph must keep the raw reply bytes and use plain application/grpc:
// Google answers 404 to application/grpc+proto.
func TestHomeGraphRequestWireFormat(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	want := wire(2, wire(1, []byte("home")))
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		if ct := md.Get("content-type"); len(ct) != 1 || ct[0] != "application/grpc" {
			t.Errorf("content-type %v", ct)
		}
		if err := stream.RecvMsg(new(emptypb.Empty)); err != nil {
			return err
		}
		reply := new(emptypb.Empty)
		reply.ProtoReflect().SetUnknown(want)
		return stream.SendMsg(reply)
	}))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	conn, err := grpc.NewClient("passthrough:///homegraph-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply := new(emptypb.Empty)
	if err := conn.Invoke(ctx, "/google.internal.home.foyer.v1.StructuresService/GetHomeGraph", new(emptypb.Empty), reply); err != nil {
		t.Fatal(err)
	}
	if string(reply.ProtoReflect().GetUnknown()) != string(want) {
		t.Fatal("wire bytes changed")
	}
}
func TestRebootIsSinglePOSTEvenWhenResponseLost(t *testing.T) {
	authEnv(t)
	m := New("")
	m.masterHash = [32]byte{}
	m.authHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return response(200, "Auth=access\n"), nil })}
	calls := 0
	m.apiHTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "POST" {
			t.Fatal("not POST")
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != "{}" {
			t.Fatal("unexpected reboot payload")
		}
		return nil, errors.New("connection lost")
	})}
	if m.Reboot(context.Background(), "test-group") == nil {
		t.Fatal("expected uncertain error")
	}
	if calls != 1 {
		t.Fatal("reboot retried")
	}
}

// Shapes match responses observed from the real Foyer API (values are fake).
var foyerFixture = map[string]string{
	"/v2/groups": `{"groups":[{"id":"g","accessPoints":[
		{"id":"root","accessPointSettings":{"accessPointOtherSettings":{"apName":"Living Room Wifi"}},
		 "accessPointProperties":{"ipAddress":"192.0.2.1","isGroupRoot":true,"hardwareType":"H2D","localAuthorizationToken":"secret-lat","serialNumber":"secret-serial","lastSeenTime":"2026-11-19T20:53:16Z"}},
		{"id":"point","accessPointSettings":{"accessPointOtherSettings":{"apName":"Kitchen Wifi"}},
		 "accessPointProperties":{"ipAddress":"192.0.2.32","hardwareType":"H2E","lastSeenTime":"2026-11-19T20:52:57Z"}}]}]}`,
	"/v2/groups/g/status": `{"wanConnectionStatus":"WAN_ONLINE","apStatuses":[{"apId":"root","apState":"AP_ONLINE"},{"apId":"point","apState":"AP_OFFLINE"}]}`,
	"/v2/groups/g/realtimeMetrics": `{"groupTraffic":{"transmitSpeedBps":"5692416","receiveSpeedBps":"37972696"},
		"stationMetrics":[{"station":{"apId":"point","connectionType":"WIRELESS","wirelessBand":"BAND_5000_MHZ","friendlyName":"Phone","ipAddress":"192.0.2.77"}},
		                  {"station":{"apId":"point","connectionType":"WIRELESS","wirelessBand":"BAND_5000_MHZ"}},
		                  {"station":{"apId":"root","connectionType":"WIRED","wirelessBand":"NOT_APPLICABLE"}}],
		"meshMetrics":[{"apId":"point","speed":{"speedRating":"OK","numericSpeedRating":"50"}}]}`,
	"/v2/groups/g/stations": `{"stations":[{"id":"s1","friendlyName":"Kids Switch","connected":true},{"id":"s2","connected":true},{"id":"s3","connected":true},{"id":"s4","connected":false}]}`,
	// Real shape: wrapped in "group"; ended pauses keep expiry == creation.
	"/v2/groups/g": `{"group":{"groupSettings":{"familyHubSettings":{"stationPolicies":[
		{"stationId":"s1","blockingPolicy":{"creationTimestamp":"2026-11-20T08:00:00Z","expiryTimestamp":"2099-01-01T00:00:00Z"}},
		{"stationId":"s2","blockingPolicy":{"creationTimestamp":"2023-11-27T20:48:01.196Z","expiryTimestamp":"2023-11-27T20:48:01.196Z"}},
		{"stationId":"s3","blockingPolicy":{"expiryTimestamp":"1970-01-01T00:00:00Z"}}]}}}}`,
}

func TestClientsPauseState(t *testing.T) {
	clients, err := foyerManager(t).Clients(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	paused := map[string]bool{}
	for _, c := range clients {
		paused[c.ID] = c.Paused
	}
	if !paused["s1"] || paused["s2"] || !paused["s3"] || paused["s4"] || clients[0].Name != "Kids Switch" {
		t.Fatalf("%+v", clients)
	}
}

func foyerManager(t *testing.T) *Manager {
	t.Helper()
	authEnv(t)
	m := New("")
	m.authHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return response(200, "Auth=access\n"), nil })}
	m.apiHTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatal("telemetry/inventory must be read-only")
		}
		if body, ok := foyerFixture[r.URL.Path]; ok {
			return response(200, body), nil
		}
		return response(404, `{}`), nil
	})}
	return m
}

func TestCheckGroupComparesIPs(t *testing.T) {
	m := foyerManager(t)
	for _, ips := range [][]string{{"192.0.2.1"}, {"192.0.2.1", "192.0.2.99"}, {"192.0.2.1", "192.0.2.32", "192.0.2.5"}} {
		if m.CheckGroup(context.Background(), "g", ips) == nil {
			t.Fatal("mismatched inventory accepted:", ips)
		}
	}
	if err := m.CheckGroup(context.Background(), "g", []string{"192.0.2.32", "192.0.2.1"}); err != nil {
		t.Fatal(err)
	}
	if m.CheckGroup(context.Background(), "", []string{"192.0.2.32", "192.0.2.1"}) == nil {
		t.Fatal("reboot check without explicit group")
	}
}

func TestTelemetryAggregatesWithoutPrivateData(t *testing.T) {
	tel, err := foyerManager(t).Telemetry(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !tel.WANOnline || !tel.APOnline["root"] || tel.APOnline["point"] || tel.Mesh["point"] != (MeshSpeed{"OK", 50}) {
		t.Fatalf("%+v", tel)
	}
	if tel.TxBps != 5692416 || tel.RxBps != 37972696 || tel.Known != 4 || tel.Connected != 3 {
		t.Fatalf("%+v", tel)
	}
	if tel.Clients[ClientKey{"point", "WIRELESS", "BAND_5000_MHZ"}] != 2 || tel.Clients[ClientKey{"root", "WIRED", "NOT_APPLICABLE"}] != 1 {
		t.Fatalf("%+v", tel.Clients)
	}
	if s := fmt.Sprintf("%+v", tel); strings.Contains(s, "secret") || strings.Contains(s, "Phone") || strings.Contains(s, "192.0.2.77") {
		t.Fatal("private data retained")
	}
	if ap := tel.Group.AccessPoints[0]; ap.Name() != "Living Room Wifi" || !ap.Properties.Root || ap.Properties.LastSeen.IsZero() {
		t.Fatalf("%+v", ap)
	}
}

func TestNetworkCauseWithoutURL(t *testing.T) {
	authEnv(t)
	m := New("")
	m.authHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return response(200, "Auth=access\n"), nil })}
	m.apiHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: lookup googlehomefoyer-pa.googleapis.com: i/o timeout")
	})}
	_, err := m.Groups(context.Background())
	if err == nil || !strings.Contains(err.Error(), "i/o timeout") || strings.Contains(err.Error(), "https://") {
		t.Fatalf("%v", err)
	}
}

func TestGraphFallsBackToCacheUnlessForced(t *testing.T) {
	m := New("")
	m.graph = []GraphDevice{{ID: "cached"}}
	m.graphExpires = time.Now().Add(-time.Minute) // expired: refresh is attempted
	authEnv(t)
	m.authHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}
	if g, err := m.Graph(context.Background(), false); err != nil || len(g) != 1 {
		t.Fatal("telemetry should use the cached graph", err)
	}
	if _, err := m.Graph(context.Background(), true); err == nil {
		t.Fatal("forced refresh must fail without the cloud")
	}
}
