package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/logger"
)

// LastRunManager handles last run timestamp management
type LastRunManager struct {
	filePath        string
	defaultLookback time.Duration
}

// NewLastRunManager creates a new last run manager
func NewLastRunManager(filePath, defaultLookback string) (*LastRunManager, error) {
	duration, err := time.ParseDuration(defaultLookback)
	if err != nil {
		return nil, fmt.Errorf("invalid default_lookback duration: %w", err)
	}

	return &LastRunManager{
		filePath:        filePath,
		defaultLookback: duration,
	}, nil
}

// GetDateRange returns the appropriate date range based on mode and last run
func (lrm *LastRunManager) GetDateRange(mode string) (startDate, endDate string, isFirstRun bool) {
	now := time.Now().UTC()
	endDate = now.Format("2006-01-02T15:04:05.000Z")

	if mode == "init" {
		// Init mode: no date filtering
		return "", "", false
	}

	// Update mode: check for last run
	lastRun, exists := lrm.getLastRun()
	if !exists {
		// First run: use default lookback
		startTime := now.Add(-lrm.defaultLookback)
		startDate = startTime.Format("2006-01-02T15:04:05.000Z")
		logger.Infof("First update run: looking back %s from %s to %s",
			lrm.defaultLookback, startDate, endDate)
		return startDate, endDate, true
	}

	// Subsequent runs: from last run to now
	startDate = lastRun.Format("2006-01-02T15:04:05.000Z")
	logger.Infof("Update run: from last run %s to %s", startDate, endDate)
	return startDate, endDate, false
}

// SaveLastRun saves the current timestamp as last run
func (lrm *LastRunManager) SaveLastRun() error {
	now := time.Now().UTC()
	timestamp := strconv.FormatInt(now.Unix(), 10)

	err := ioutil.WriteFile(lrm.filePath, []byte(timestamp), 0644)
	if err != nil {
		return fmt.Errorf("failed to save last run timestamp: %w", err)
	}

	logger.Infof("Saved last run timestamp: %s", now.Format("2006-01-02T15:04:05Z"))
	return nil
}

// getLastRun reads the last run timestamp from file
func (lrm *LastRunManager) getLastRun() (time.Time, bool) {
	data, err := ioutil.ReadFile(lrm.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("No last run file found - this appears to be the first update run")
		} else {
			logger.WithError(err).Warn("Failed to read last run file")
		}
		return time.Time{}, false
	}

	timestamp, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		logger.WithError(err).Warn("Failed to parse last run timestamp")
		return time.Time{}, false
	}

	lastRun := time.Unix(timestamp, 0).UTC()
	return lastRun, true
}

// GetLastRunInfo returns human-readable last run information
func (lrm *LastRunManager) GetLastRunInfo() string {
	lastRun, exists := lrm.getLastRun()
	if !exists {
		return "No previous run found"
	}

	since := time.Since(lastRun)
	return fmt.Sprintf("Last run: %s (%s ago)",
		lastRun.Format("2006-01-02 15:04:05 UTC"),
		since.Truncate(time.Minute))
}

// SetupDateRangeForMode configures the API config with appropriate date ranges
func SetupDateRangeForMode(cfg *Config) error {
	if cfg.General.Mode == "init" {
		logger.Info("Running in INIT mode - full synchronization (no date filtering)")
		return nil
	}

	logger.Info("Running in UPDATE mode - incremental synchronization")

	// Create last run manager
	lrm, err := NewLastRunManager(cfg.General.LastRunFile, cfg.General.DefaultLookback)
	if err != nil {
		return fmt.Errorf("failed to create last run manager: %w", err)
	}

	// Get date range for update mode
	startDate, endDate, isFirstRun := lrm.GetDateRange(cfg.General.Mode)

	// Set date range in API config
	cfg.NVD.API.LastModStartDate = startDate
	cfg.NVD.API.LastModEndDate = endDate

	// Log information
	logger.Infof("Update window: %s", lrm.GetLastRunInfo())
	if isFirstRun {
		logger.Infof("First update run - looking back %s", cfg.General.DefaultLookback)
	}

	return nil
}

// FinishUpdateRun saves the current timestamp and logs completion
func FinishUpdateRun(cfg *Config) error {
	if cfg.General.Mode != "update" {
		return nil // Only save timestamp for update mode
	}

	lrm, err := NewLastRunManager(cfg.General.LastRunFile, cfg.General.DefaultLookback)
	if err != nil {
		return fmt.Errorf("failed to create last run manager: %w", err)
	}

	if err := lrm.SaveLastRun(); err != nil {
		return fmt.Errorf("failed to save last run: %w", err)
	}

	logger.Info("Update run completed successfully - timestamp saved for next run")
	return nil
}
