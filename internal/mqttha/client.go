// Package mqttha publishes to MQTT using Home Assistant discovery.
package mqttha

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
)

// Client keeps a connection to the broker. Topics passed to Publish and
// Subscribe are relative to the configured topic prefix.
type Client struct {
	cfg    config.MQTT
	paho   paho.Client
	status string // availability topic

	mu        sync.Mutex
	onConnect []func()
	subs      map[string]func(payload []byte)
}

// Handler receives non-retained command messages only: a retained command
// (e.g. left on the broker by mistake) must not trigger a reboot on reconnect.
type Handler func(payload []byte)

func password() (string, error) {
	if path := os.Getenv("MQTT_PASSWORD_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return "", nil // optional Secret not created: anonymous/no password
		}
		if err != nil {
			return "", errors.New("cannot read MQTT password file")
		}
		return strings.TrimSpace(string(b)), nil
	}
	return os.Getenv("MQTT_PASSWORD"), nil
}

func tlsConfig(c config.MQTT) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: c.TLS.InsecureSkipVerify}
	if c.TLS.CAFile != "" {
		pem, err := os.ReadFile(c.TLS.CAFile)
		if err != nil {
			return nil, errors.New("cannot read MQTT CA file")
		}
		t.RootCAs = x509.NewCertPool()
		if !t.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("invalid MQTT CA file")
		}
	}
	if c.TLS.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if err != nil {
			return nil, errors.New("cannot load MQTT client certificate")
		}
		t.Certificates = []tls.Certificate{cert}
	}
	return t, nil
}

// Connect starts a connection that keeps retrying in the background; it does
// not fail when the broker is temporarily unreachable.
func Connect(c config.MQTT, binary string) (*Client, error) {
	pw, err := password()
	if err != nil {
		return nil, err
	}
	t, err := tlsConfig(c)
	if err != nil {
		return nil, err
	}
	m := &Client{cfg: c, status: c.TopicPrefix + "/" + binary + "/status", subs: map[string]func([]byte){}}
	id := c.ClientID
	if id == "" {
		id = "nestwifi-" + binary
	}
	opts := paho.NewClientOptions().AddBroker(c.URL).SetClientID(id).
		SetUsername(c.Username).SetPassword(pw).SetTLSConfig(t).
		SetCleanSession(true).SetAutoReconnect(true).SetMaxReconnectInterval(time.Minute).
		SetWill(m.status, "offline", 1, true).
		SetOnConnectHandler(func(paho.Client) { go m.connected() }).
		SetConnectionLostHandler(func(_ paho.Client, err error) { slog.Warn("MQTT connection lost", "error", err) })
	m.paho = paho.NewClient(opts)
	// paho's built-in initial-connect retry is silent; retry here so a wrong
	// broker address or credentials show up in the logs. Later drops are
	// handled by auto-reconnect.
	go func() {
		for wait := 5 * time.Second; ; wait = min(2*wait, time.Minute) {
			t := m.paho.Connect()
			if t.Wait() && t.Error() == nil {
				return
			}
			slog.Warn("MQTT connect failed; retrying", "broker", c.URL, "error", t.Error(), "in", wait)
			time.Sleep(wait)
		}
	}()
	return m, nil
}

// connected (re)publishes availability, discovery and subscriptions. It also
// runs when Home Assistant restarts and announces itself on <discovery>/status.
func (m *Client) connected() {
	slog.Info("MQTT connected", "broker", m.cfg.URL)
	m.publishRaw(m.status, true, []byte("online"))
	m.paho.Subscribe(m.cfg.DiscoveryPrefix+"/status", 1, func(_ paho.Client, msg paho.Message) {
		if string(msg.Payload()) == "online" {
			go m.announce()
		}
	})
	m.mu.Lock()
	for topic, h := range m.subs {
		m.subscribe(topic, h)
	}
	m.mu.Unlock()
	m.announce()
}

func (m *Client) announce() {
	m.mu.Lock()
	callbacks := append([]func(){}, m.onConnect...)
	m.mu.Unlock()
	for _, f := range callbacks {
		f()
	}
}

// OnConnect registers f to run after every (re)connection and HA restart,
// typically to publish discovery and current state.
func (m *Client) OnConnect(f func()) {
	m.mu.Lock()
	m.onConnect = append(m.onConnect, f)
	m.mu.Unlock()
	if m.paho.IsConnectionOpen() {
		go f()
	}
}

func (m *Client) subscribe(topic string, h func([]byte)) {
	m.paho.Subscribe(topic, 1, func(_ paho.Client, msg paho.Message) {
		if msg.Retained() {
			slog.Warn("ignoring retained MQTT command", "topic", msg.Topic())
			return
		}
		h(msg.Payload())
	})
}

// Subscribe registers a command handler (re-subscribed after reconnects).
func (m *Client) Subscribe(topic string, h Handler) {
	topic = m.Topic(topic)
	m.mu.Lock()
	m.subs[topic] = h
	m.mu.Unlock()
	if m.paho.IsConnectionOpen() {
		m.subscribe(topic, h)
	}
}

func (m *Client) Topic(rel string) string { return m.cfg.TopicPrefix + "/" + rel }

// Availability is the topic carrying online/offline for this process.
func (m *Client) Availability() string { return m.status }

func (m *Client) publishRaw(topic string, retained bool, payload []byte) {
	if !m.paho.IsConnectionOpen() {
		return // state is re-published on reconnect
	}
	tok := m.paho.Publish(topic, 1, retained, payload)
	go func() {
		if tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
			slog.Warn("MQTT publish failed", "topic", topic, "error", tok.Error())
		}
	}()
}

// Publish sends retained JSON state to a topic relative to the prefix.
func (m *Client) Publish(rel string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("MQTT payload encoding failed", "topic", rel, "error", err)
		return
	}
	m.publishRaw(m.Topic(rel), true, b)
}

// Discover publishes a Home Assistant device discovery message.
func (m *Client) Discover(d Device) {
	b, err := json.Marshal(d)
	if err != nil {
		slog.Error("MQTT discovery encoding failed", "error", err)
		return
	}
	m.publishRaw(m.cfg.DiscoveryPrefix+"/device/"+d.ObjectID+"/config", true, b)
}

// Close marks the process offline and disconnects.
func (m *Client) Close() {
	if m.paho.IsConnectionOpen() {
		m.paho.Publish(m.status, 1, true, "offline").WaitTimeout(2 * time.Second)
	}
	m.paho.Disconnect(500)
}
