package strategies

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
	"github.com/luhtaf/nvd-fetcher/internal/models"
	"github.com/luhtaf/nvd-fetcher/internal/stage1"
	"github.com/luhtaf/nvd-fetcher/internal/stage2"
	"github.com/luhtaf/nvd-fetcher/internal/stage3"
	"github.com/luhtaf/nvd-fetcher/internal/stage3/factory"
)

// BasePipeline contains shared pipeline functionality
type BasePipeline struct {
	config  *config.Config
	fetcher *stage1.Fetcher
	parser  *stage2.Parser
	indexer *stage3.Indexer

	// State management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Runtime statistics
	stats struct {
		mu           sync.RWMutex
		startTime    time.Time
		endTime      time.Time
		totalFetched int64
		totalParsed  int64
		totalIndexed int64
		totalErrors  int64
	}
}

// NewBasePipeline creates a new base pipeline
func NewBasePipeline(cfg *config.Config) (*BasePipeline, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create pipeline stages
	fetcher := stage1.New(cfg)
	parser := stage2.New(cfg)

	// Determine indexer strategy
	var indexerStrategy factory.IndexerType
	switch cfg.Workers.Stage3Indexer.Strategy {
	case "bulk":
		indexerStrategy = factory.BulkIndexer
	case "streaming":
		indexerStrategy = factory.StreamingIndexer
	case "parallel":
		indexerStrategy = factory.ParallelIndexer
	default:
		indexerStrategy = factory.BulkIndexer
		logger.Warnf("Unknown indexer strategy '%s', using 'bulk'", cfg.Workers.Stage3Indexer.Strategy)
	}

	indexer, err := stage3.New(cfg, indexerStrategy)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create indexer: %w", err)
	}

	return &BasePipeline{
		config:  cfg,
		fetcher: fetcher,
		parser:  parser,
		indexer: indexer,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// StartStages starts all pipeline stages
func (p *BasePipeline) StartStages() error {
	logger.Info("Starting pipeline stages...")

	// Start indexer first
	if err := p.indexer.Start(); err != nil {
		return fmt.Errorf("failed to start indexer: %w", err)
	}

	// Start parser
	if err := p.parser.Start(); err != nil {
		return fmt.Errorf("failed to start parser: %w", err)
	}

	// Start fetcher last
	if err := p.fetcher.Start(); err != nil {
		return fmt.Errorf("failed to start fetcher: %w", err)
	}

	logger.Info("All pipeline stages started successfully")
	return nil
}

// ConnectStages connects the output of one stage to the input of the next
func (p *BasePipeline) ConnectStages() {
	logger.Info("Connecting pipeline stages...")

	// Connect fetcher → parser
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for fetchResult := range p.fetcher.GetResultChannel() {
			if !p.parser.AddTask(fetchResult) {
				logger.Warn("Parser queue full, dropping fetch result")
			}
		}
		logger.Debug("Fetcher→Parser connection closed")
	}()

	// Connect parser → indexer
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for parseResult := range p.parser.GetResultChannel() {
			if !p.indexer.AddTask(parseResult) {
				logger.Warn("Indexer queue full, dropping parse result")
			}
		}
		logger.Debug("Parser→Indexer connection closed")
	}()

	logger.Info("Pipeline stages connected")
}

// ResultCollector collects final results from indexer
func (p *BasePipeline) ResultCollector() {
	defer p.wg.Done()

	for indexResult := range p.indexer.GetResultChannel() {
		p.updateStats(indexResult)

		if indexResult.Error != "" {
			logger.WithField("page", indexResult.Page).WithField("error", indexResult.Error).
				Error("Index operation failed")
		} else if indexResult.SuccessCount > 0 {
			logger.WithField("page", indexResult.Page).
				WithField("success", indexResult.SuccessCount).
				WithField("failed", indexResult.FailedCount).
				Debug("Index operation completed")
		}
	}

	logger.Debug("Result collector finished")
}

// StatsMonitor periodically logs pipeline statistics
func (p *BasePipeline) StatsMonitor() {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.logFinalStats()
			return
		case <-ticker.C:
			p.logCurrentStats()
		}
	}
}

// WaitForCompletion waits for pipeline completion
func (p *BasePipeline) WaitForCompletion() <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		// Wait for fetcher to complete
		fetcherDone := make(chan struct{})
		go func() {
			defer close(fetcherDone)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()

			var lastProcessed int64 = 0
			stableCount := 0

			for {
				select {
				case <-p.ctx.Done():
					return
				case <-ticker.C:
					status := p.fetcher.GetStatus()

					// Check if reached max pages limit
					if status.TotalProcessed >= int64(p.config.General.MaxPages) && p.config.General.MaxPages > 0 {
						logger.Infof("Fetcher reached max pages limit: %d", p.config.General.MaxPages)
						return
					}

					// Check if fetcher stopped processing (natural completion)
					if status.TotalProcessed == lastProcessed {
						stableCount++
						if stableCount >= 3 { // 15 seconds of no progress
							logger.Infof("Fetcher appears to have completed naturally at %d pages", status.TotalProcessed)
							return
						}
					} else {
						stableCount = 0
						lastProcessed = status.TotalProcessed
					}
				}
			}
		}()

		<-fetcherDone
		logger.Info("Fetcher completed, waiting for pipeline to drain...")
		time.Sleep(10 * time.Second)
	}()

	return done
}

// StopStages stops all pipeline stages gracefully
func (p *BasePipeline) StopStages() error {
	logger.Info("Stopping pipeline stages...")

	timeout := 30 * time.Second
	var stopErrors []error

	if err := p.fetcher.Stop(timeout); err != nil {
		logger.WithError(err).Error("Failed to stop fetcher")
		stopErrors = append(stopErrors, err)
	}

	if err := p.parser.Stop(timeout); err != nil {
		logger.WithError(err).Error("Failed to stop parser")
		stopErrors = append(stopErrors, err)
	}

	if err := p.indexer.Stop(timeout); err != nil {
		logger.WithError(err).Error("Failed to stop indexer")
		stopErrors = append(stopErrors, err)
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All pipeline goroutines finished")
	case <-time.After(timeout):
		logger.Warn("Timeout waiting for pipeline goroutines to finish")
	}

	if len(stopErrors) > 0 {
		return fmt.Errorf("errors stopping stages: %v", stopErrors)
	}

	logger.Info("All pipeline stages stopped successfully")
	return nil
}

// Helper methods

func (p *BasePipeline) updateStats(result *models.IndexResult) {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()

	if result.Error != "" {
		p.stats.totalErrors++
	} else {
		p.stats.totalIndexed += int64(result.SuccessCount)
		if result.FailedCount > 0 {
			p.stats.totalErrors += int64(result.FailedCount)
		}
	}
}

func (p *BasePipeline) logCurrentStats() {
	p.stats.mu.RLock()
	runtime := time.Since(p.stats.startTime)
	totalFetched := p.stats.totalFetched
	totalParsed := p.stats.totalParsed
	totalIndexed := p.stats.totalIndexed
	totalErrors := p.stats.totalErrors
	p.stats.mu.RUnlock()

	// Get stage status
	fetcherStatus := p.fetcher.GetStatus()
	parserStatus := p.parser.GetStatus()
	indexerStatus := p.indexer.GetStatus()

	logger.Infof("=== Pipeline Status (Runtime: %v) ===", runtime.Truncate(time.Second))
	logger.Infof("Fetcher: %d processed, %d errors, buffer: %d/%d",
		fetcherStatus.TotalProcessed, fetcherStatus.TotalErrors,
		fetcherStatus.ResultBufferUsed, fetcherStatus.ResultBufferSize)
	logger.Infof("Parser: %d processed, %d errors, buffer: %d/%d",
		parserStatus.TotalProcessed, parserStatus.TotalErrors,
		parserStatus.ResultBufferUsed, parserStatus.ResultBufferSize)
	logger.Infof("Indexer: %d processed, %d errors, buffer: %d/%d",
		indexerStatus.TotalProcessed, indexerStatus.TotalErrors,
		indexerStatus.ResultBufferUsed, indexerStatus.ResultBufferSize)
	logger.Infof("Total: %d fetched, %d parsed, %d indexed, %d errors",
		totalFetched, totalParsed, totalIndexed, totalErrors)
}

func (p *BasePipeline) logFinalStats() {
	p.stats.mu.Lock()
	p.stats.endTime = time.Now()
	runtime := p.stats.endTime.Sub(p.stats.startTime)
	totalFetched := p.stats.totalFetched
	totalParsed := p.stats.totalParsed
	totalIndexed := p.stats.totalIndexed
	totalErrors := p.stats.totalErrors
	p.stats.mu.Unlock()

	logger.Infof("=== Pipeline Final Statistics ===")
	logger.Infof("Total runtime: %v", runtime.Truncate(time.Second))
	logger.Infof("Pages fetched: %d", totalFetched)
	logger.Infof("CVEs parsed: %d", totalParsed)
	logger.Infof("Documents indexed: %d", totalIndexed)
	logger.Infof("Total errors: %d", totalErrors)

	if runtime > 0 {
		fetchRate := float64(totalFetched) / runtime.Seconds()
		parseRate := float64(totalParsed) / runtime.Seconds()
		indexRate := float64(totalIndexed) / runtime.Seconds()

		logger.Infof("Rates: %.2f pages/sec, %.2f CVEs/sec, %.2f docs/sec",
			fetchRate, parseRate, indexRate)
	}
}
