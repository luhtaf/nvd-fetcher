package stage3

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
	"github.com/luhtaf/nvd-fetcher/internal/models"
	"github.com/luhtaf/nvd-fetcher/internal/stage3/factory"
)

// Indexer handles indexing of parsed CVE data to Elasticsearch
type Indexer struct {
	config   *config.Config
	strategy factory.IndexerStrategy

	// Channels
	taskChan   chan *models.IndexTask
	resultChan chan *models.IndexResult

	// Bulk buffer management
	bulkBuffer []*models.ESDocument
	bufferMu   sync.Mutex
	lastFlush  time.Time

	// State management
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running int32

	// Statistics
	stats struct {
		mu             sync.RWMutex
		totalProcessed int64
		totalErrors    int64
		totalIndexed   int64
		totalFailed    int64
		bulkOps        int64
		avgProcessTime time.Duration
		lastActivity   time.Time
	}
}

// New creates a new indexer instance
func New(cfg *config.Config, strategyType factory.IndexerType) (*Indexer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create indexer factory and strategy
	f := factory.NewFactory(cfg)
	strategy, err := f.CreateIndexer(strategyType)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create indexer strategy: %w", err)
	}

	indexer := &Indexer{
		config:     cfg,
		strategy:   strategy,
		taskChan:   make(chan *models.IndexTask, cfg.Workers.Stage3Indexer.BufferSize),
		resultChan: make(chan *models.IndexResult, cfg.Workers.Stage3Indexer.BufferSize),
		bulkBuffer: make([]*models.ESDocument, 0, cfg.Workers.Stage3Indexer.BulkSize),
		ctx:        ctx,
		cancel:     cancel,
		lastFlush:  time.Now(),
	}

	return indexer, nil
}

// Start starts the indexer workers
func (idx *Indexer) Start() error {
	if !atomic.CompareAndSwapInt32(&idx.running, 0, 1) {
		return fmt.Errorf("indexer is already running")
	}

	logger.Infof("Starting %d indexer workers with %s strategy",
		idx.config.Workers.Stage3Indexer.Count, idx.strategy.Name())

	// Start worker goroutines
	for i := 0; i < idx.config.Workers.Stage3Indexer.Count; i++ {
		idx.wg.Add(1)
		go idx.worker(i + 1)
	}

	// Start bulk flusher goroutine
	idx.wg.Add(1)
	go idx.bulkFlusher()

	return nil
}

// Stop stops the indexer workers
func (idx *Indexer) Stop(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&idx.running, 1, 0) {
		return nil // Already stopped
	}

	logger.Info("Stopping indexer workers...")

	// Flush any remaining bulk operations
	idx.flushBulkBuffer(true)

	// Cancel context to signal workers to stop
	idx.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		idx.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All indexer workers stopped gracefully")
	case <-time.After(timeout):
		logger.Warn("Indexer workers stop timeout exceeded")
	}

	// Close indexer strategy
	if err := idx.strategy.Close(); err != nil {
		logger.WithError(err).Warn("Failed to close indexer strategy")
	}

	// Close channels
	close(idx.taskChan)
	close(idx.resultChan)

	// Print final statistics
	idx.printFinalStats()

	return nil
}

// AddTask adds an index task to the queue
func (idx *Indexer) AddTask(parseResult *models.ParseResult) bool {
	if atomic.LoadInt32(&idx.running) == 0 {
		return false
	}

	task := &models.IndexTask{
		ParseResult: parseResult,
	}

	select {
	case idx.taskChan <- task:
		return true
	default:
		return false // Channel is full
	}
}

// GetResultChannel returns the result channel for reading index results
func (idx *Indexer) GetResultChannel() <-chan *models.IndexResult {
	return idx.resultChan
}

// GetStatus returns the current status of the indexer
func (idx *Indexer) GetStatus() models.StageStatus {
	idx.stats.mu.RLock()
	defer idx.stats.mu.RUnlock()

	return models.StageStatus{
		Name:             fmt.Sprintf("indexer-%s", idx.strategy.Name()),
		WorkersRunning:   idx.config.Workers.Stage3Indexer.Count,
		ActiveWorkers:    idx.getActiveWorkerCount(),
		TaskBufferSize:   cap(idx.taskChan),
		TaskBufferUsed:   len(idx.taskChan),
		ResultBufferSize: cap(idx.resultChan),
		ResultBufferUsed: len(idx.resultChan),
		TotalProcessed:   idx.stats.totalProcessed,
		TotalErrors:      idx.stats.totalErrors,
		AvgProcessTime:   idx.stats.avgProcessTime,
		LastActivity:     idx.stats.lastActivity,
	}
}

// worker is the main worker goroutine that processes index tasks
func (idx *Indexer) worker(workerID int) {
	defer idx.wg.Done()

	logger.Debugf("Indexer worker %d started", workerID)

	for {
		select {
		case <-idx.ctx.Done():
			logger.Debugf("Indexer worker %d stopping due to context cancellation", workerID)
			return
		case task, ok := <-idx.taskChan:
			if !ok {
				logger.Debugf("Indexer worker %d stopping due to closed task channel", workerID)
				return
			}

			if task == nil {
				continue
			}

			startTime := time.Now()
			result := idx.processTask(workerID, task)
			duration := time.Since(startTime)
			result.Duration = duration

			// Update statistics
			idx.updateStats(duration, result.Error != "", result.SuccessCount, result.FailedCount)

			// Send result
			select {
			case idx.resultChan <- result:
			case <-idx.ctx.Done():
				return
			}
		}
	}
}

// processTask processes a single index task
func (idx *Indexer) processTask(workerID int, task *models.IndexTask) *models.IndexResult {
	logger.Debugf("Worker %d processing page %d", workerID, task.ParseResult.Page)

	if task.ParseResult.Error != "" || len(task.ParseResult.CVEs) == 0 {
		return &models.IndexResult{
			Page:         task.ParseResult.Page,
			SuccessCount: 0,
			FailedCount:  0,
			Error:        task.ParseResult.Error,
		}
	}

	// Convert CVEs to Elasticsearch documents
	documents := make([]*models.ESDocument, 0, len(task.ParseResult.CVEs))
	for _, cve := range task.ParseResult.CVEs {
		doc := idx.createESDocument(cve)
		documents = append(documents, doc)
	}

	// Add to bulk buffer or index immediately
	if idx.config.Workers.Stage3Indexer.BulkSize > 1 {
		idx.addToBulkBuffer(documents)
		// Return preliminary result
		return &models.IndexResult{
			Page:         task.ParseResult.Page,
			SuccessCount: len(documents),
			FailedCount:  0,
		}
	} else {
		// Index immediately for bulk_size = 1
		indexResult, err := idx.strategy.IndexDocuments(documents)
		if err != nil {
			return &models.IndexResult{
				Page:         task.ParseResult.Page,
				SuccessCount: 0,
				FailedCount:  len(documents),
				Error:        err.Error(),
			}
		}

		return &models.IndexResult{
			Page:         task.ParseResult.Page,
			SuccessCount: indexResult.SuccessCount,
			FailedCount:  indexResult.FailedCount,
		}
	}
}

// createESDocument creates an Elasticsearch document from ParsedCVE
func (idx *Indexer) createESDocument(cve *models.ParsedCVE) *models.ESDocument {
	indexName := strings.ReplaceAll(idx.config.Elasticsearch.IndexTemplate, "{year}", cve.Year)

	return &models.ESDocument{
		Index:  indexName,
		ID:     cve.CVEID,
		Source: cve.Data,
	}
}

// addToBulkBuffer adds documents to the bulk buffer
func (idx *Indexer) addToBulkBuffer(documents []*models.ESDocument) {
	idx.bufferMu.Lock()
	defer idx.bufferMu.Unlock()

	idx.bulkBuffer = append(idx.bulkBuffer, documents...)

	// Check if buffer is full
	if len(idx.bulkBuffer) >= idx.config.Workers.Stage3Indexer.BulkSize {
		idx.flushBulkBufferLocked()
	}
}

// bulkFlusher periodically flushes the bulk buffer
func (idx *Indexer) bulkFlusher() {
	defer idx.wg.Done()

	ticker := time.NewTicker(idx.config.Workers.Stage3Indexer.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-idx.ctx.Done():
			logger.Debug("Bulk flusher stopping due to context cancellation")
			return
		case <-ticker.C:
			idx.flushBulkBuffer(false)
		}
	}
}

// flushBulkBuffer flushes the bulk buffer to Elasticsearch
func (idx *Indexer) flushBulkBuffer(force bool) {
	idx.bufferMu.Lock()
	defer idx.bufferMu.Unlock()
	idx.flushBulkBufferLocked()
}

// flushBulkBufferLocked flushes the bulk buffer (must be called with bufferMu locked)
func (idx *Indexer) flushBulkBufferLocked() {
	if len(idx.bulkBuffer) == 0 {
		return
	}

	// Check if we should flush
	timeSinceLastFlush := time.Since(idx.lastFlush)
	if len(idx.bulkBuffer) < idx.config.Workers.Stage3Indexer.BulkSize &&
		timeSinceLastFlush < idx.config.Workers.Stage3Indexer.FlushInterval {
		return
	}

	// Copy buffer and reset
	documentsToIndex := make([]*models.ESDocument, len(idx.bulkBuffer))
	copy(documentsToIndex, idx.bulkBuffer)
	idx.bulkBuffer = idx.bulkBuffer[:0] // Reset slice but keep capacity
	idx.lastFlush = time.Now()

	// Index documents outside of lock
	go func() {
		result, err := idx.strategy.IndexDocuments(documentsToIndex)
		if err != nil {
			logger.WithError(err).Errorf("Failed to index bulk of %d documents", len(documentsToIndex))
			idx.updateStats(0, true, 0, len(documentsToIndex))
			return
		}

		if len(result.Errors) > 0 {
			logger.Warnf("Bulk operation: %d success, %d failed", result.SuccessCount, result.FailedCount)
			for i, err := range result.Errors {
				if i < 5 { // Log first 5 errors
					logger.Warnf("Failed item: %+v", err)
				}
			}
		} else {
			logger.Debugf("Bulk operation: %d documents indexed successfully", result.SuccessCount)
		}

		idx.updateStats(0, false, result.SuccessCount, result.FailedCount)

		// Update bulk operation count
		idx.stats.mu.Lock()
		idx.stats.bulkOps++
		idx.stats.mu.Unlock()
	}()
}

// updateStats updates internal statistics
func (idx *Indexer) updateStats(duration time.Duration, hasError bool, successCount, failedCount int) {
	idx.stats.mu.Lock()
	defer idx.stats.mu.Unlock()

	idx.stats.totalProcessed++
	if hasError {
		idx.stats.totalErrors++
	}

	idx.stats.totalIndexed += int64(successCount)
	idx.stats.totalFailed += int64(failedCount)

	// Update average process time
	if duration > 0 {
		if idx.stats.avgProcessTime == 0 {
			idx.stats.avgProcessTime = duration
		} else {
			idx.stats.avgProcessTime = (idx.stats.avgProcessTime + duration) / 2
		}
	}

	idx.stats.lastActivity = time.Now()
}

// getActiveWorkerCount returns the number of active workers (approximation)
func (idx *Indexer) getActiveWorkerCount() int {
	if atomic.LoadInt32(&idx.running) == 0 {
		return 0
	}
	return idx.config.Workers.Stage3Indexer.Count
}

// printFinalStats prints final indexing statistics
func (idx *Indexer) printFinalStats() {
	idx.stats.mu.RLock()
	defer idx.stats.mu.RUnlock()

	logger.Infof("=== Indexer Final Stats ===")
	logger.Infof("Strategy: %s", idx.strategy.Name())
	logger.Infof("Total indexed: %d", idx.stats.totalIndexed)
	logger.Infof("Total failed: %d", idx.stats.totalFailed)
	logger.Infof("Bulk operations: %d", idx.stats.bulkOps)
	logger.Infof("Total errors: %d", idx.stats.totalErrors)
}
