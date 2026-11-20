// Package mqtttest provides an in-process MQTT broker for tests.
package mqtttest

import (
	"net"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

type Broker struct {
	*mochi.Server
	URL  string
	mu   sync.Mutex
	last map[string]string // topic -> last payload
}

func Start(t *testing.T) *Broker {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	b := &Broker{Server: mochi.New(&mochi.Options{InlineClient: true}), URL: "tcp://" + addr, last: map[string]string{}}
	if err := b.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "test", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go b.Serve()
	t.Cleanup(func() { b.Close() })
	b.Server.Subscribe("#", 1, func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
		b.mu.Lock()
		b.last[pk.TopicName] = string(pk.Payload)
		b.mu.Unlock()
	})
	return b
}

// Last returns the last payload seen on topic.
func (b *Broker) Last(topic string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last[topic]
}

// Topics returns all topics seen so far.
func (b *Broker) Topics() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.last))
	for t := range b.last {
		out = append(out, t)
	}
	return out
}

// Wait polls cond for up to five seconds.
func Wait(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("timed out waiting for MQTT condition")
}
