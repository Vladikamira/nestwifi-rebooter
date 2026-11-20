package mqttha

import (
	"regexp"
	"strings"
)

// Device is a Home Assistant device-based discovery payload (HA 2024.11+):
// one retained message per device listing all of its entities.
type Device struct {
	ObjectID     string               `json:"-"`
	Dev          DeviceInfo           `json:"dev"`
	Origin       map[string]string    `json:"o"`
	Components   map[string]Component `json:"cmps"`
	StateTopic   string               `json:"state_topic,omitempty"`
	Availability string               `json:"availability_topic,omitempty"`
}

type DeviceInfo struct {
	IDs          []string `json:"ids"`
	Name         string   `json:"name,omitempty"`
	Manufacturer string   `json:"mf,omitempty"`
	Model        string   `json:"mdl,omitempty"`
	Software     string   `json:"sw,omitempty"`
	ViaDevice    string   `json:"via_device,omitempty"`
	ConfigURL    string   `json:"cu,omitempty"`
}

// Component is one entity. Only the fields used here are modelled.
type Component struct {
	Platform         string   `json:"p"`
	UniqueID         string   `json:"unique_id"`
	Name             string   `json:"name"`
	DeviceClass      string   `json:"device_class,omitempty"`
	StateClass       string   `json:"state_class,omitempty"`
	Unit             string   `json:"unit_of_measurement,omitempty"`
	EntityCategory   string   `json:"entity_category,omitempty"`
	Icon             string   `json:"icon,omitempty"`
	ValueTemplate    string   `json:"value_template,omitempty"`
	StateTopic       string   `json:"state_topic,omitempty"`
	CommandTopic     string   `json:"command_topic,omitempty"`
	PayloadOn        string   `json:"payload_on,omitempty"`
	PayloadOff       string   `json:"payload_off,omitempty"`
	PayloadPress     string   `json:"payload_press,omitempty"`
	EnabledByDefault *bool    `json:"enabled_by_default,omitempty"`
	Schema           string   `json:"schema,omitempty"`     // light: json
	Brightness       bool     `json:"brightness,omitempty"` // light
	BrightnessScale  int      `json:"brightness_scale,omitempty"`
	SupportedModes   []string `json:"supported_color_modes,omitempty"`
}

var nonID = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

// ID makes a topic- and unique_id-safe identifier.
func ID(parts ...string) string {
	return strings.Trim(nonID.ReplaceAllString(strings.Join(parts, "_"), "_"), "_")
}

// Sensor helpers keep entity definitions in callers short.

func Sensor(id, name, template string) Component {
	return Component{Platform: "sensor", UniqueID: id, Name: name, ValueTemplate: template}
}

// Binary maps a JSON boolean to ON/OFF.
func Binary(id, name, field, class string) Component {
	return Component{Platform: "binary_sensor", UniqueID: id, Name: name, DeviceClass: class,
		ValueTemplate: "{{ 'ON' if value_json." + field + " else 'OFF' }}", PayloadOn: "ON", PayloadOff: "OFF"}
}

func (c Component) Diagnostic() Component { c.EntityCategory = "diagnostic"; return c }
func (c Component) Class(device, state, unit string) Component {
	c.DeviceClass, c.StateClass, c.Unit = device, state, unit
	return c
}
func (c Component) Disabled() Component            { f := false; c.EnabledByDefault = &f; return c }
func (c Component) WithIcon(icon string) Component { c.Icon = icon; return c }
