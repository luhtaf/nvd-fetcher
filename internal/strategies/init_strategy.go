package strategies

import (
	"context"
	"fmt"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
)

// InitStrategy handles full data synchronization
type InitStrategy struct {
	pipeline *BasePipeline
}

// NewInitStrategy creates a new init strategy
func NewInitStrategy() *InitStrategy {
	return &InitStrategy{}
}

// Name returns the strategy name
func (s *InitStrategy) Name() string {
	return "init"
}

// GetDescription returns description of init strategy
func (s *InitStrategy) GetDescription() string {
	return "Full synchronization - downloads all available CVE data from NVD"
}

// Prepare setups the init strategy
func (s *InitStrategy) Prepare(cfg *config.Config) error {
	logger.Info("🚀 INIT Strategy: Preparing full data synchronization...")
	logger.Info("Mode: Full sync (no date filtering)")
	logger.Infof("Target: All available CVE data (~311K entries)")

	// No date range setup needed for init mode
	cfg.NVD.API.LastModStartDate = ""
	cfg.NVD.API.LastModEndDate = ""

	// Create pipeline
	pipeline, err := NewBasePipeline(cfg)
	if err != nil {
		return fmt.Errorf("failed to create init pipeline: %w", err)
	}
	s.pipeline = pipeline

	return nil
}

// Execute runs the init strategy
func (s *InitStrategy) Execute(ctx context.Context, cfg *config.Config) error {
	logger.Info("🔄 INIT Strategy: Starting full synchronization...")
	s.pipeline.stats.startTime = time.Now()

	// Start all stages
	if err := s.pipeline.StartStages(); err != nil {
		return fmt.Errorf("failed to start pipeline stages: %w", err)
	}

	// Connect pipeline stages
	s.pipeline.ConnectStages()

	// Start monitoring
	s.pipeline.wg.Add(1)
	go s.pipeline.StatsMonitor()

	s.pipeline.wg.Add(1)
	go s.pipeline.ResultCollector()

	// Wait for completion
	select {
	case <-ctx.Done():
		logger.Info("Init strategy cancelled by context")
	case <-s.pipeline.WaitForCompletion():
		logger.Info("Init strategy completed naturally")
	}

	// Stop stages
	return s.pipeline.StopStages()
}

// Cleanup performs cleanup after init strategy
func (s *InitStrategy) Cleanup(cfg *config.Config) error {
	logger.Info("✅ INIT Strategy: Full synchronization completed")
	// No special cleanup for init mode (no last_run file to update)
	return nil
}
