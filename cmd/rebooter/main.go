package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/Vladikamira/nestwifi-rebooter/internal/cloud"
	"github.com/Vladikamira/nestwifi-rebooter/internal/config"
	"github.com/Vladikamira/nestwifi-rebooter/internal/device"
	"github.com/Vladikamira/nestwifi-rebooter/internal/discovery"
	"github.com/Vladikamira/nestwifi-rebooter/internal/mqttha"
	"github.com/Vladikamira/nestwifi-rebooter/internal/rebooter"
	"github.com/Vladikamira/nestwifi-rebooter/internal/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/robfig/cron/v3"
)

func run() error {
	path := flag.String("config", "/config/config.yaml", "configuration file")
	enabled := flag.Bool("enable-reboot", false, "explicitly allow scheduled/once reboot (default is read-only preflight)")
	once := flag.Bool("once", false, "perform one job and exit; pending recovery takes precedence")
	list := flag.Bool("list-groups", false, "list group IDs and AP counts, without changes")
	discoverOnly := flag.Bool("discover", false, "print read-only discovery inventory and exit")
	flag.Parse()
	c, err := config.Load(*path)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	auth := cloud.New(c.HomeID)
	var source *discovery.Resolver
	if c.Discovery.Enabled {
		source = discovery.New(c, auth)
	}
	if *discoverOnly {
		if source == nil {
			return fmt.Errorf("enable discovery in configuration first")
		}
		result, err := source.Discover(ctx, true)
		if err != nil {
			slog.Warn("cloud discovery incomplete; local results only", "error", err)
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	if *list {
		task, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		groups, err := auth.Groups(task)
		if err != nil {
			return err
		}
		for _, g := range groups {
			fmt.Printf("group_id=%s access_points=%d\n", g.ID, len(g.AccessPoints))
			for _, ap := range g.AccessPoints {
				fmt.Printf("  %-15s %-4s root=%-5t %s\n", ap.Properties.IP, ap.Properties.Hardware, ap.Properties.Root, ap.Name())
			}
		}
		return nil
	}
	if *enabled && c.Rebooter.GroupID == "" {
		return fmt.Errorf("group_id is required with --enable-reboot")
	}
	store, err := rebooter.OpenStore(c.Rebooter.StateDir)
	if err != nil {
		return err
	}
	defer store.Close()
	registry := prometheus.NewRegistry()
	auth.RegisterMetrics(registry)
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	runner := rebooter.New(c, store, auth, device.New(auth), *enabled, registry)
	if source != nil {
		runner.SetDiscovery(source)
	}
	if *once {
		// Run handles unfinished state first and never reboots in that case.
		return runner.Run(ctx)
	}
	// Recover even without --enable-reboot: disabling future jobs must not leave
	// speakers quiet after an earlier operation. Recovery never sends reboot.
	if err := runner.Recover(ctx); err != nil {
		slog.Error("startup recovery incomplete", "error", err)
	}
	controller := rebooter.NewController(ctx, runner, auth, c.Rebooter.GroupID, c.Rebooter.ManagedClients, registry)
	if c.Rebooter.SpeedtestDelay > 0 {
		runner.AfterSuccess(controller.AfterReboot(c.Rebooter.SpeedtestDelay))
	}
	if c.MQTT.URL != "" {
		m, err := mqttha.Connect(c.MQTT, "rebooter")
		if err != nil {
			return err
		}
		defer m.Close()
		controller.Attach(m)
		go controller.Run(ctx)
	}
	location, _ := time.LoadLocation(c.Rebooter.Timezone)
	scheduler := cron.New(cron.WithLocation(location), cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)))
	var entry cron.EntryID
	entry, err = scheduler.AddFunc(c.Rebooter.Schedule, func() {
		// cron has already advanced Next when the job starts.
		runner.SetNextRun(scheduler.Entry(entry).Next)
		if err := runner.Scheduled(ctx); err != nil {
			slog.Error("scheduled job failed", "error", err)
		}
	})
	if err != nil {
		return err
	}
	scheduler.Start()
	defer func() { <-scheduler.Stop().Done() }()
	runner.SetNextRun(scheduler.Entry(entry).Next)
	var recovery sync.WaitGroup
	recovery.Add(1)
	defer recovery.Wait()
	go func() {
		defer recovery.Done()
		ticker := time.NewTicker(c.Rebooter.RecoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := runner.Recover(ctx); err != nil {
					slog.Warn("recovery remains pending", "error", err)
				}
			}
		}
	}()
	slog.Info("rebooter started", "enabled", *enabled, "schedule", c.Rebooter.Schedule, "timezone", c.Rebooter.Timezone)
	err = server.Run(ctx, c.Listen, registry, runner.Ready)
	cancel()
	return err
}
func main() {
	if err := run(); err != nil {
		slog.Error("rebooter stopped", "error", err)
		os.Exit(1)
	}
}
