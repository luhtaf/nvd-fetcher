package strategies

import (
	"context"
	"fmt"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
)

// UpdateStrategy handles incremental data synchronization
type UpdateStrategy struct {
	pipeline *BasePipeline
}

// NewUpdateStrategy creates a new update strategy
func NewUpdateStrategy() *UpdateStrategy {
	return &UpdateStrategy{}
}

// Name returns the strategy name
func (s *UpdateStrategy) Name() string {
	return "update"
}

// GetDescription returns description of update strategy
func (s *UpdateStrategy) GetDescription() string {
	return "Incremental synchronization - downloads only new/updated CVE data since last run"
}

// Prepare setups the update strategy
func (s *UpdateStrategy) Prepare(cfg *config.Config) error {
	logger.Info("🔄 UPDATE Strategy: Preparing incremental synchronization...")

	// Setup date range for update mode using existing config logic
	if err := config.SetupDateRangeForMode(cfg); err != nil {
		return fmt.Errorf("failed to setup date range for update mode: %w", err)
	}

	logger.Infof("Date range: %s to %s", cfg.NVD.API.LastModStartDate, cfg.NVD.API.LastModEndDate)

	// Create pipeline
	pipeline, err := NewBasePipeline(cfg)
	if err != nil {
		return fmt.Errorf("failed to create update pipeline: %w", err)
	}
	s.pipeline = pipeline

	return nil
}

// Execute runs the update strategy
func (s *UpdateStrategy) Execute(ctx context.Context, cfg *config.Config) error {
	logger.Info("🔄 UPDATE Strategy: Starting incremental synchronization...")
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
		logger.Info("Update strategy cancelled by context")
	case <-s.pipeline.WaitForCompletion():
		logger.Info("Update strategy completed naturally")
	}

	// Stop stages
	return s.pipeline.StopStages()
}

// Cleanup performs cleanup after update strategy
func (s *UpdateStrategy) Cleanup(cfg *config.Config) error {
	logger.Info("✅ UPDATE Strategy: Incremental synchronization completed")

	// Save last run timestamp for future updates
	if err := config.FinishUpdateRun(cfg); err != nil {
		return fmt.Errorf("failed to save last run timestamp: %w", err)
	}

	return nil
}
