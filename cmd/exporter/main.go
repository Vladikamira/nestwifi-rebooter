package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/Vladikamira/nestwifi-rebooter/internal/exporter"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
	"github.com/Vladikamira/nestwifi-rebooter/internal/server"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	path := flag.String("config", "/config/config.yaml", "configuration file")
	discoverOnly := flag.Bool("discover", false, "print read-only discovery inventory and exit")
	flag.Parse()

	c, err := config.Load(*path)
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.Info("Initializing home " + c.HomeID)
	auth := cloud.New(c.HomeID)
	var source *discovery.Resolver
	if c.Discovery.Enabled {
		source = discovery.New(c, auth)
	}

	if *discoverOnly {
		if source == nil {
			slog.Error("enable discovery in configuration first")
			os.Exit(1)
		}
		result, err := source.Discover(ctx, true)
		if err != nil {
			slog.Warn("cloud discovery incomplete; local results only", "error", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			os.Exit(1)
		}
		return
	}

	client := device.New(auth)
	collector := exporter.New(c, client)
	if source != nil {
		collector.SetDiscovery(source)
	}

	registry := prometheus.NewRegistry()
	auth.RegisterMetrics(registry)
	registry.MustRegister(collector, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	go collector.Run(ctx)
	var cloudCollector *exporter.CloudCollector
	if c.Exporter.CloudInterval > 0 {
		cloudCollector = exporter.NewCloud(auth, c.Rebooter.GroupID, c.Exporter.CloudInterval)
		registry.MustRegister(cloudCollector)
		go cloudCollector.Run(ctx)
	}
	if c.MQTT.URL != "" {
		if cloudCollector == nil {
			slog.Error("MQTT publishing needs cloud telemetry; set exporter.cloud_interval > 0")
			os.Exit(1)
		}
		m, err := mqttha.Connect(c.MQTT, "exporter")
		if err != nil {
			slog.Error("MQTT setup failed", "error", err)
			os.Exit(1)
		}
		defer m.Close()
		go exporter.NewPublisher(m, collector, cloudCollector, c.Exporter.Interval).Run(ctx)
	}
	slog.Info("exporter started", "listen", c.Listen, "devices", len(c.Devices))

	if err := server.Run(ctx, c.Listen, registry, collector.Ready); err != nil {
		slog.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
