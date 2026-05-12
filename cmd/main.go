package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/dipherent1/grand-opus/config"
	"github.com/dipherent1/grand-opus/crawler"
	"github.com/dipherent1/grand-opus/pkg"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Change to slog.LevelDebug to see all link discovery logs

	}))
	slog.SetDefault(logger) // Make it the default logger across the application

	cfg := config.LoadConfig()

	go func() {
		logger.Info("Starting Prometheus metrics server", "address", ":2112/metrics")
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			logger.Error("Metrics server failed to start", "error", err)
		}
	}()

	client, err := pkg.ConnectToMongoDB(cfg.MongoURI)
	db := client.Database(cfg.MongoDB)

	if err != nil {
		logger.Error("Failed to connect to MongoDB:", "error", err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			logger.Error("Error disconnecting from MongoDB:", "error", err)
		}
	}()
	appCrawler := crawler.NewCrawler(db, cfg, logger)
	go func() {
		logger.Info("Crawler started in the background.")
		appCrawler.Crawl() // This will now run concurrently
		logger.Info("Crawler has finished its work.")
	}()

	// Create a channel to listen for OS signals.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// This is a blocking read, which keeps the main function alive.
	<-shutdown

	logger.Info("Shutdown signal received. Exiting gracefully.")
}
