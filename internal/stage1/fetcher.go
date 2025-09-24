package stage1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/logger"
	"github.com/luhtaf/nvd-fetcher/internal/models"
	"github.com/luhtaf/nvd-fetcher/internal/ratelimit"
)

// Fetcher handles fetching data from NVD API
type Fetcher struct {
	config      *config.Config
	rateLimiter *ratelimit.RateLimiter
	client      *http.Client

	// Channels
	taskChan   chan *models.FetchTask
	resultChan chan *models.FetchResult

	// State management
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      int32
	totalResults int64
	pageCounter  int64

	// Statistics
	stats struct {
		mu             sync.RWMutex
		totalProcessed int64
		totalErrors    int64
		avgProcessTime time.Duration
		lastActivity   time.Time
	}
}

// New creates a new fetcher instance
func New(cfg *config.Config) *Fetcher {
	ctx, cancel := context.WithCancel(context.Background())

	// Auto-adjust rate limit based on API key availability
	rateLimit := cfg.NVD.API.RateLimit.RequestsPerSecond
	burstSize := cfg.NVD.API.RateLimit.BurstSize

	if cfg.NVD.API.APIKey != "" {
		logger.Info("API key detected - using enhanced rate limits")
		// With API key: 50 requests per 30 seconds = 1.67 req/sec
		if rateLimit <= 0.2 { // If using default low rate
			rateLimit = 1.67
			burstSize = 5
			logger.Infof("Auto-adjusted rate limit to %f req/sec (burst: %d) for API key usage", rateLimit, burstSize)
		}
	} else {
		logger.Warn("No API key - using conservative rate limits (5 req/30sec)")
		// Without API key: stick to 5 requests per 30 seconds = 0.167 req/sec
		if rateLimit > 0.2 { // If accidentally set too high
			rateLimit = 0.167
			burstSize = 2
			logger.Warnf("Rate limit too high for no API key usage, adjusted to %f req/sec (burst: %d)", rateLimit, burstSize)
		}
	}

	return &Fetcher{
		config:      cfg,
		rateLimiter: ratelimit.New(rateLimit, burstSize),
		client: &http.Client{
			Timeout: time.Duration(cfg.NVD.API.Timeout) * time.Second,
		},
		taskChan:    make(chan *models.FetchTask, cfg.Workers.Stage1Fetcher.BufferSize),
		resultChan:  make(chan *models.FetchResult, cfg.Workers.Stage1Fetcher.BufferSize),
		ctx:         ctx,
		cancel:      cancel,
		pageCounter: 1,
	}
}

// Start starts the fetcher workers
func (f *Fetcher) Start() error {
	if !atomic.CompareAndSwapInt32(&f.running, 0, 1) {
		return fmt.Errorf("fetcher is already running")
	}

	logger.Infof("Starting %d fetcher workers", f.config.Workers.Stage1Fetcher.Count)

	// Start worker goroutines
	for i := 0; i < f.config.Workers.Stage1Fetcher.Count; i++ {
		f.wg.Add(1)
		go f.worker(i + 1)
	}

	// Start task generator
	f.wg.Add(1)
	go f.taskGenerator()

	return nil
}

// Stop stops the fetcher workers
func (f *Fetcher) Stop(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&f.running, 1, 0) {
		return nil // Already stopped
	}

	logger.Info("Stopping fetcher workers...")

	// Cancel context to signal workers to stop
	f.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All fetcher workers stopped gracefully")
	case <-time.After(timeout):
		logger.Warn("Fetcher workers stop timeout exceeded")
	}

	// Close channels
	close(f.taskChan)
	close(f.resultChan)

	return nil
}

// GetResultChannel returns the result channel for reading fetch results
func (f *Fetcher) GetResultChannel() <-chan *models.FetchResult {
	return f.resultChan
}

// GetStatus returns the current status of the fetcher
func (f *Fetcher) GetStatus() models.StageStatus {
	f.stats.mu.RLock()
	defer f.stats.mu.RUnlock()

	return models.StageStatus{
		Name:             "fetcher",
		WorkersRunning:   f.config.Workers.Stage1Fetcher.Count,
		ActiveWorkers:    f.getActiveWorkerCount(),
		TaskBufferSize:   cap(f.taskChan),
		TaskBufferUsed:   len(f.taskChan),
		ResultBufferSize: cap(f.resultChan),
		ResultBufferUsed: len(f.resultChan),
		TotalProcessed:   f.stats.totalProcessed,
		TotalErrors:      f.stats.totalErrors,
		AvgProcessTime:   f.stats.avgProcessTime,
		LastActivity:     f.stats.lastActivity,
	}
}

// worker is the main worker goroutine that processes fetch tasks
func (f *Fetcher) worker(workerID int) {
	defer f.wg.Done()

	logger.Debugf("Fetcher worker %d started", workerID)

	for {
		select {
		case <-f.ctx.Done():
			logger.Debugf("Fetcher worker %d stopping due to context cancellation", workerID)
			return
		case task, ok := <-f.taskChan:
			if !ok {
				logger.Debugf("Fetcher worker %d stopping due to closed task channel", workerID)
				return
			}

			if task == nil {
				continue
			}

			startTime := time.Now()
			result := f.processTask(workerID, task)
			duration := time.Since(startTime)
			result.Duration = duration

			// Update statistics
			f.updateStats(duration, result.Error != "")

			// Send result
			select {
			case f.resultChan <- result:
			case <-f.ctx.Done():
				return
			}
		}
	}
}

// taskGenerator generates fetch tasks automatically
func (f *Fetcher) taskGenerator() {
	defer f.wg.Done()

	logger.Debug("Task generator started")

	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			logger.Debug("Task generator stopping due to context cancellation")
			return
		case <-ticker.C:
			// Check if we need to generate more tasks
			if f.shouldGenerateTasks() {
				task := f.getNextTask()
				if task == nil {
					continue // No more tasks to generate
				}

				select {
				case f.taskChan <- task:
					logger.Debugf("Generated task for page %d", task.Page)
				case <-f.ctx.Done():
					return
				default:
					// Channel is full, try again later
				}
			}
		}
	}
}

// processTask processes a single fetch task
func (f *Fetcher) processTask(workerID int, task *models.FetchTask) *models.FetchResult {
	logger.Debugf("Worker %d processing page %d", workerID, task.Page)

	// Apply rate limiting
	if err := f.waitForRateLimit(workerID); err != nil {
		return &models.FetchResult{
			Page:  task.Page,
			Error: fmt.Sprintf("rate limiter error: %v", err),
		}
	}

	// Build request
	req, err := f.buildRequest(task)
	if err != nil {
		return &models.FetchResult{
			Page:  task.Page,
			Error: fmt.Sprintf("failed to create request: %v", err),
		}
	}

	// Execute request with retries
	resp, err := f.executeRequest(workerID, req)
	if err != nil {
		return &models.FetchResult{
			Page:  task.Page,
			Error: err.Error(),
		}
	}
	defer resp.Body.Close()

	// Process response
	return f.processResponse(task, resp)
}

// waitForRateLimit handles rate limiting logic
func (f *Fetcher) waitForRateLimit(workerID int) error {
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()

	logger.Debugf("Worker %d: Waiting for rate limiter...", workerID)
	waitStart := time.Now()

	if err := f.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	waitDuration := time.Since(waitStart)
	if waitDuration > 100*time.Millisecond {
		logger.Debugf("Worker %d: Rate limiter wait took %v", workerID, waitDuration)
	}

	return nil
}

// buildRequest creates HTTP request with proper headers and URL
func (f *Fetcher) buildRequest(task *models.FetchTask) (*http.Request, error) {
	url := f.buildURL(task)

	req, err := http.NewRequestWithContext(f.ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("User-Agent", "nvd-elastic-feed/1.0")
	req.Header.Set("Accept", "application/json")

	// Add API key if available
	if f.config.NVD.API.APIKey != "" {
		req.Header.Set("apiKey", f.config.NVD.API.APIKey)
	}

	return req, nil
}

// buildURL constructs the API URL with parameters
func (f *Fetcher) buildURL(task *models.FetchTask) string {
	url := fmt.Sprintf("%s?startIndex=%d&resultsPerPage=%d",
		f.config.NVD.API.BaseURL, task.StartIndex, f.config.NVD.API.PerPage)

	// Add date range parameters for update mode
	if f.config.NVD.API.LastModStartDate != "" {
		url += "&lastModStartDate=" + f.config.NVD.API.LastModStartDate
	}
	if f.config.NVD.API.LastModEndDate != "" {
		url += "&lastModEndDate=" + f.config.NVD.API.LastModEndDate
	}

	return url
}

// executeRequest performs HTTP request with retry logic
func (f *Fetcher) executeRequest(workerID int, req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var lastErr error

	for attempt := 0; attempt < f.config.NVD.API.RetryAttempts; attempt++ {
		resp, lastErr = f.client.Do(req)
		if lastErr == nil && resp.StatusCode < 500 {
			break // Success or client error (don't retry)
		}

		if resp != nil {
			resp.Body.Close()
		}

		if attempt < f.config.NVD.API.RetryAttempts-1 {
			time.Sleep(f.config.NVD.API.RetryDelay * time.Duration(attempt+1))
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("request failed after %d attempts: %v", f.config.NVD.API.RetryAttempts, lastErr)
	}

	return resp, nil
}

// processResponse handles HTTP response parsing
func (f *Fetcher) processResponse(task *models.FetchTask, resp *http.Response) *models.FetchResult {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &models.FetchResult{
			Page:       task.Page,
			Error:      fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)),
			StatusCode: resp.StatusCode,
		}
	}

	// Read and parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &models.FetchResult{
			Page:  task.Page,
			Error: fmt.Sprintf("failed to read response body: %v", err),
		}
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return &models.FetchResult{
			Page:  task.Page,
			Error: fmt.Sprintf("failed to parse JSON response: %v", err),
		}
	}

	// Update total results if this is the first successful response
	f.updateTotalResults(data)

	return &models.FetchResult{
		Page:       task.Page,
		Data:       data,
		StatusCode: resp.StatusCode,
	}
}

// updateTotalResults atomically updates total results count
func (f *Fetcher) updateTotalResults(data map[string]interface{}) {
	if totalResults, ok := data["totalResults"].(float64); ok {
		if atomic.LoadInt64(&f.totalResults) == 0 {
			atomic.StoreInt64(&f.totalResults, int64(totalResults))
			logger.Infof("Total results detected: %d", int64(totalResults))
		}
	}
}

// shouldGenerateTasks checks if we should generate more tasks
func (f *Fetcher) shouldGenerateTasks() bool {
	// Check if task channel has space
	if len(f.taskChan) >= cap(f.taskChan)/2 {
		return false
	}

	// Check if we've reached the limit
	totalResults := atomic.LoadInt64(&f.totalResults)
	if totalResults > 0 {
		currentPage := atomic.LoadInt64(&f.pageCounter)
		startIndex := (currentPage - 1) * int64(f.config.NVD.API.PerPage)
		if startIndex >= totalResults {
			return false
		}
	}

	// Check max pages limit
	if f.config.General.MaxPages > 0 {
		currentPage := atomic.LoadInt64(&f.pageCounter)
		if currentPage > int64(f.config.General.MaxPages) {
			return false
		}
	}

	return true
}

// getNextTask gets the next task to process
func (f *Fetcher) getNextTask() *models.FetchTask {
	currentPage := atomic.AddInt64(&f.pageCounter, 1)

	return &models.FetchTask{
		Page:       int(currentPage - 1),
		StartIndex: int((currentPage - 2) * int64(f.config.NVD.API.PerPage)),
		PerPage:    f.config.NVD.API.PerPage,
	}
}

// updateStats updates internal statistics
func (f *Fetcher) updateStats(duration time.Duration, hasError bool) {
	f.stats.mu.Lock()
	defer f.stats.mu.Unlock()

	f.stats.totalProcessed++
	if hasError {
		f.stats.totalErrors++
	}

	// Update average process time
	if f.stats.avgProcessTime == 0 {
		f.stats.avgProcessTime = duration
	} else {
		f.stats.avgProcessTime = (f.stats.avgProcessTime + duration) / 2
	}

	f.stats.lastActivity = time.Now()
}

// getActiveWorkerCount returns the number of active workers (approximation)
func (f *Fetcher) getActiveWorkerCount() int {
	if atomic.LoadInt32(&f.running) == 0 {
		return 0
	}
	return f.config.Workers.Stage1Fetcher.Count
}

// GetTotalResults returns the total number of results detected
func (f *Fetcher) GetTotalResults() int64 {
	return atomic.LoadInt64(&f.totalResults)
}
