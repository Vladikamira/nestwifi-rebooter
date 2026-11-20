package device

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

const receiverNS = "urn:x-cast:com.google.cast.receiver"
const connectionNS = "urn:x-cast:com.google.cast.tp.connection"
const heartbeatNS = "urn:x-cast:com.google.cast.tp.heartbeat"

type Volume struct {
	Level float64 `json:"level"`
	Muted bool    `json:"muted"`
}
type castConn struct {
	net.Conn
	id int
}

func textField(b []byte, n protowire.Number, s string) []byte {
	b = protowire.AppendTag(b, n, protowire.BytesType)
	return protowire.AppendString(b, s)
}
func castFrame(namespace string, payload []byte) []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, 0)
	b = textField(b, 2, "sender-nestwifi")
	b = textField(b, 3, "receiver-0")
	b = textField(b, 4, namespace)
	b = protowire.AppendTag(b, 5, protowire.VarintType)
	b = protowire.AppendVarint(b, 0)
	return textField(b, 6, string(payload))
}
func castPayload(b []byte) (string, []byte, error) {
	var ns string
	var payload []byte
	for len(b) > 0 {
		n, t, k := protowire.ConsumeTag(b)
		if k < 0 {
			return "", nil, errors.New("invalid Cast frame")
		}
		b = b[k:]
		if t == protowire.BytesType {
			v, k := protowire.ConsumeBytes(b)
			if k < 0 {
				return "", nil, errors.New("invalid Cast frame")
			}
			if n == 4 {
				ns = string(v)
			}
			if n == 6 {
				payload = v
			}
			b = b[k:]
		} else {
			k := protowire.ConsumeFieldValue(n, t, b)
			if k < 0 {
				return "", nil, errors.New("invalid Cast frame")
			}
			b = b[k:]
		}
	}
	return ns, payload, nil
}
func (c *castConn) send(ns string, v any) error {
	p, err := json.Marshal(v)
	if err != nil {
		return err
	}
	frame := castFrame(ns, p)
	buf := make([]byte, 4, len(frame)+4)
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	buf = append(buf, frame...)
	_, err = io.Copy(c.Conn, bytes.NewReader(buf))
	return err
}
func (c *castConn) request(kind string, volume any) (Volume, error) {
	c.id++
	p := map[string]any{"type": kind, "requestId": c.id}
	if volume != nil {
		p["volume"] = volume
	}
	if err := c.send(receiverNS, p); err != nil {
		return Volume{}, errors.New("Cast write failed")
	}
	for {
		var header [4]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return Volume{}, errors.New("Cast read failed")
		}
		size := binary.BigEndian.Uint32(header[:])
		if size == 0 || size > 1<<20 {
			return Volume{}, errors.New("invalid Cast frame size")
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(c.Conn, buf); err != nil {
			return Volume{}, errors.New("Cast read failed")
		}
		ns, payload, err := castPayload(buf)
		if err != nil {
			return Volume{}, err
		}
		var msg struct {
			Type   string `json:"type"`
			ID     int    `json:"requestId"`
			Status struct {
				Volume *Volume `json:"volume"`
			} `json:"status"`
		}
		if json.Unmarshal(payload, &msg) != nil {
			continue
		}
		if ns == heartbeatNS && msg.Type == "PING" {
			if err := c.send(heartbeatNS, map[string]string{"type": "PONG"}); err != nil {
				return Volume{}, errors.New("Cast heartbeat failed")
			}
		}
		if ns == receiverNS && msg.ID == c.id {
			if msg.Type != "RECEIVER_STATUS" || msg.Status.Volume == nil {
				return Volume{}, errors.New("Cast status rejected")
			}
			return *msg.Status.Volume, nil
		}
	}
}
func castSession(ctx context.Context, ip string) (*castConn, func(), error) {
	// Cast devices use self-signed certificates on the trusted LAN.
	tlsConfig := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: tlsConfig}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip, "8009"))
	if err != nil {
		return nil, nil, errors.New("Cast connection failed")
	}
	deadline := time.Now().Add(12 * time.Second)
	if t, ok := ctx.Deadline(); ok && t.Before(deadline) {
		deadline = t
	}
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	cleanup := func() { stop(); _ = conn.Close() }
	c := &castConn{Conn: conn}
	if err := c.send(connectionNS, map[string]string{"type": "CONNECT"}); err != nil {
		cleanup()
		return nil, nil, errors.New("Cast CONNECT failed")
	}
	return c, cleanup, nil
}
func ReadVolume(ctx context.Context, ip string) (Volume, error) {
	c, close, err := castSession(ctx, ip)
	if err != nil {
		return Volume{}, err
	}
	defer close()
	return c.request("GET_STATUS", nil)
}
func RestoreVolume(ctx context.Context, ip string, v Volume) error {
	c, close, err := castSession(ctx, ip)
	if err != nil {
		return err
	}
	defer close()
	if _, err = c.request("SET_VOLUME", map[string]any{"level": v.Level}); err != nil {
		return err
	}
	if _, err = c.request("SET_VOLUME", map[string]any{"muted": v.Muted}); err != nil {
		return err
	}
	got, err := c.request("GET_STATUS", nil)
	if err != nil {
		return err
	}
	if !VolumeEqual(got, v) {
		return errors.New("Cast volume restore not confirmed")
	}
	return nil
}
func VolumeEqual(a, b Volume) bool {
	d := a.Level - b.Level
	return a.Muted == b.Muted && d < 0.001 && d > -0.001
}
