package main

import (
	"context"
	"log"
	"strconv"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// SysBase is the topic prefix for all Mosquitto SYS messages.
const SysBase = "$SYS/broker/"

var instrumentMap map[string]any
var currentInts map[string]int64
var currentFloats map[string]float64

func initInstruments() (err error) {
	meter = otel.Meter("mosquitto")

	instrumentMap = make(map[string]any)
	instrumentMap["clients/connected"], err = meter.Int64UpDownCounter("mqtt/clients/connected")
	instrumentMap["clients/disconnected"], err = meter.Int64UpDownCounter("mqtt/clients/disconnected")
	instrumentMap["clients/expired"], err = meter.Int64UpDownCounter("mqtt/clients/expired")
	instrumentMap["clients/maximum"], err = meter.Int64Counter("mqtt/clients/maximum")
	instrumentMap["clients/total"], err = meter.Int64UpDownCounter("mqtt/clients/total")
	instrumentMap["load/bytes/received/1min"], err = meter.Float64UpDownCounter("mqtt/load/bytes/received/1min")
	instrumentMap["load/bytes/sent/1min"], err = meter.Float64UpDownCounter("mqtt/load/bytes/sent/1min")
	instrumentMap["load/messages/received/1min"], err = meter.Float64UpDownCounter("mqtt/load/messages/received/1min")
	instrumentMap["load/messages/sent/1min"], err = meter.Float64UpDownCounter("mqtt/load/messages/sent/1min")
	instrumentMap["load/publish/dropped/1min"], err = meter.Float64UpDownCounter("mqtt/load/publish/dropped/1min")
	instrumentMap["load/publish/received/1min"], err = meter.Float64UpDownCounter("mqtt/load/publish/received/1min")
	instrumentMap["load/publish/sent/1min"], err = meter.Float64UpDownCounter("mqtt/load/publish/sent/1min")
	instrumentMap["load/sockets/1min"], err = meter.Float64UpDownCounter("mqtt/load/sockets/1min")
	instrumentMap["load/connections/1min"], err = meter.Float64UpDownCounter("mqtt/load/connections/1min")
	instrumentMap["heap/current"], err = meter.Int64UpDownCounter("mqtt/heap/current")
	instrumentMap["heap/maximum"], err = meter.Int64UpDownCounter("mqtt/heap/maximum")
	instrumentMap["store/messages/count"], err = meter.Int64UpDownCounter("mqtt/store/messages/count")
	instrumentMap["store/messages/bytes"], err = meter.Int64UpDownCounter("mqtt/store/messages/bytes")
	instrumentMap["subscriptions/count"], err = meter.Int64UpDownCounter("mqtt/subscriptions/count")

	currentInts = make(map[string]int64)
	currentFloats = make(map[string]float64)
	return
}

func handleSys(_ mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if !strings.HasPrefix(topic, SysBase) {
		return
	}
	topic = strings.TrimPrefix(topic, SysBase)

	err := processSys(topic, msg.Payload())

	if err != nil {
		log.Printf("Failed to handle sys message '%s' with payload '%s': %v", SysBase+topic, msg.Payload(), err)
	}
}

func processSys(topic string, payload []byte) error {
	instrument, ok := instrumentMap[topic]
	if !ok {
		return nil
	}

	switch instrument.(type) {
	case metric.Int64UpDownCounter:
		value, err := strconv.ParseInt(string(payload), 10, 64)
		if err != nil {
			return err
		}
		current, ok := currentInts[topic]
		if !ok {
			currentInts[topic] = value
			instrument.(metric.Int64UpDownCounter).Add(context.Background(), value)
			break
		}
		instrument.(metric.Int64UpDownCounter).Add(context.Background(), value-current)
		currentInts[topic] = value
	case metric.Int64Counter:
		value, err := strconv.ParseInt(string(payload), 10, 64)
		if err != nil {
			return err
		}
		current, ok := currentInts[topic]
		if !ok {
			currentInts[topic] = value
			instrument.(metric.Int64Counter).Add(context.Background(), value)
			break
		}
		if value < current {
			currentInts[topic] = value
			instrumentMap[topic], err = meter.Int64Counter(topic)
			instrumentMap[topic].(metric.Int64Counter).Add(context.Background(), value)
			break
		}
		instrument.(metric.Int64Counter).Add(context.Background(), value-current)
		currentInts[topic] = value
	case metric.Float64UpDownCounter:
		value, err := strconv.ParseFloat(string(payload), 64)
		if err != nil {
			return err
		}
		current, ok := currentFloats[topic]
		if !ok {
			currentFloats[topic] = value
			instrument.(metric.Float64UpDownCounter).Add(context.Background(), value)
			break
		}
		instrument.(metric.Float64UpDownCounter).Add(context.Background(), value-current)
		currentFloats[topic] = value
	case metric.Float64Counter:
		value, err := strconv.ParseFloat(string(payload), 64)
		if err != nil {
			return err
		}
		current, ok := currentFloats[topic]
		if !ok {
			currentFloats[topic] = value
			instrument.(metric.Float64Counter).Add(context.Background(), value)
			break
		}
		if value < current {
			currentFloats[topic] = value
			instrumentMap[topic], err = meter.Float64Counter(topic)
			instrumentMap[topic].(metric.Float64Counter).Add(context.Background(), value)
			break
		}
		instrument.(metric.Float64Counter).Add(context.Background(), value-current)
		currentFloats[topic] = value
	}
	return nil
}
