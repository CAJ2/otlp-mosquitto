package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

func main() {
	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// Handle SIGINT (CTRL+C) gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()
	log.Printf("OpenTelemetry initialized")

	viper.SetDefault("mqtt.host", "localhost")
	mqttHost := viper.GetString("mqtt.host")
	viper.SetDefault("mqtt.port", 1883)
	mqttPort := viper.GetInt("mqtt.port")
	viper.SetDefault("mqtt.username", "username")
	mqttUser := viper.GetString("mqtt.username")
	viper.SetDefault("mqtt.password", "password")
	mqttPass := viper.GetString("mqtt.password")

	mqttOpts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", mqttHost, mqttPort)).
		SetUsername(mqttUser).
		SetPassword(mqttPass).
		SetClientID("oltp-mosquitto").
		SetProtocolVersion(3).
		SetAutoReconnect(false).
		SetOnConnectHandler(onConnect).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Fatalf("MQTT Connection lost: %s", err)
		})
	client := mqtt.NewClient(mqttOpts)
	token := client.Connect()
	if token.Error() != nil {
		return fmt.Errorf("Could not connect to MQTT: %s", token.Error())
	}
	if !token.Wait() {
		return fmt.Errorf("Could not connect to MQTT: %s", token.Error())
	}
	if !client.IsConnected() {
		return fmt.Errorf("Could not connect to MQTT: %s", token.Error())
	}

	<-ctx.Done()

	return
}

func onConnect(client mqtt.Client) {
	err := initInstruments()
	if err != nil {
		log.Printf("Could not initialize instruments: %s", err)
	}
	sysToken := client.Subscribe("$SYS/broker/#", 0, handleSys)
	if sysToken.Error() != nil {
		log.Printf("Could not subscribe to $SYS/broker/#: %s", sysToken.Error())
		return
	}
	if !sysToken.Wait() {
		log.Printf("Could not subscribe to $SYS/broker/#: %s", sysToken.Error())
		return
	}
	log.Printf("Subscribed to $SYS/broker/#")

	err = initMsgQueue()
	if err != nil {
		log.Printf("Could not initialize msg queue: %s", err)
	}
}
