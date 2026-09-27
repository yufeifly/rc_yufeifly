package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/yufeifly/rc_yufeifly/internal/notifier"
)

func main() {
	configPath := flag.String("config", "configs/config.example.yaml", "path to notifier YAML configuration")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*configPath, log); err != nil {
		log.Error("notifier stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, log *slog.Logger) error {
	cfg, err := notifier.LoadConfig(configPath)
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(); err != nil {
		return err
	}

	store, err := notifier.OpenStore(context.Background(), cfg.Database)
	if err != nil {
		return err
	}
	defer store.Close()

	providers, err := notifier.NewProviderRegistry(cfg)
	if err != nil {
		return err
	}
	auth, err := notifier.NewAuthenticator(cfg.Auth)
	if err != nil {
		return err
	}

	providerLimits := make(map[string]int, len(cfg.Providers))
	for id, provider := range cfg.Providers {
		if provider.Enabled {
			providerLimits[id] = provider.MaxConcurrency
		}
	}
	metrics := notifier.NewMetrics(prometheus.NewRegistry(), cfg.Workers.Concurrency, providerLimits)
	service := notifier.NewService(store, cfg, nil)
	scheduler := notifier.NewScheduler(cfg, store, providers, metrics, log)
	service.SetWake(scheduler.Wake)
	api := notifier.NewHTTPAPI(service, auth, cfg.Server.MaxBodyBytes)
	api.SetMetrics(metrics)
	api.SetReady(false)

	listener, err := net.Listen("tcp", cfg.Server.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
	}
	rootContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	scheduler.Start(context.Background())
	api.SetReady(true)
	log.Info("notifier started", "address", listener.Addr().String())

	refreshDone := make(chan struct{})
	go refreshMetrics(rootContext, metrics, store, log, refreshDone)
	serveError := make(chan error, 1)
	go func() {
		serveError <- server.Serve(listener)
	}()

	select {
	case <-rootContext.Done():
	case err := <-serveError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stopSignals()
			return err
		}
	}

	api.SetReady(false)
	grace := cfg.Server.ShutdownGrace
	if grace <= 0 {
		grace = 15 * time.Second
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()

	httpDone := make(chan error, 1)
	go func() {
		httpDone <- server.Shutdown(shutdownContext)
	}()
	schedulerError := scheduler.Shutdown(shutdownContext)
	httpError := <-httpDone
	<-refreshDone
	if schedulerError != nil {
		return schedulerError
	}
	if httpError != nil {
		return httpError
	}
	log.Info("notifier stopped gracefully")
	return nil
}

func refreshMetrics(ctx context.Context, metrics *notifier.Metrics, store *notifier.Store, log *slog.Logger, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		if err := metrics.RefreshStore(ctx, store, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("refresh store metrics", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
