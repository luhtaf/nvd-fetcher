package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
	"github.com/luhtaf/nvd-fetcher/internal/strategies"
)

const (
	defaultConfigPath = "config.yaml"
	appName           = "nvd-elastic-feed"
	appVersion        = "1.0.0"
)

func main() {
	// Print application header
	fmt.Printf("=== %s v%s ===\n", appName, appVersion)
	fmt.Printf("Go runtime: %s\n", runtime.Version())
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	// Load configuration
	configPath := defaultConfigPath
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := logger.InitLogger(cfg.General.LogLevel, cfg.General.LogFormat); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Create strategy factory
	factory := strategies.NewStrategyFactory()

	// Create strategy for the configured mode
	strategy, err := factory.CreateStrategy(cfg.General.Mode)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create strategy")
	}

	logger.Infof("Selected strategy: %s", strategy.Name())
	logger.Infof("Description: %s", strategy.GetDescription())

	// Prepare strategy
	if err := strategy.Prepare(cfg); err != nil {
		logger.WithError(err).Fatal("Failed to prepare strategy")
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Infof("Received signal %s, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Execute strategy
	if err := strategy.Execute(ctx, cfg); err != nil {
		logger.WithError(err).Error("Strategy execution failed")
		os.Exit(1)
	}

	// Cleanup strategy
	if err := strategy.Cleanup(cfg); err != nil {
		logger.WithError(err).Warn("Strategy cleanup failed")
	}

	logger.Info("Pipeline completed successfully")
}
