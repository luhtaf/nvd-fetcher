package stage2

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
)

// Parser handles parsing of CVE data from NVD
type Parser struct {
	config *config.Config

	// Channels
	taskChan   chan *models.ParseTask
	resultChan chan *models.ParseResult

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
		avgProcessTime time.Duration
		lastActivity   time.Time
	}
}

// New creates a new parser instance
func New(cfg *config.Config) *Parser {
	ctx, cancel := context.WithCancel(context.Background())

	return &Parser{
		config:     cfg,
		taskChan:   make(chan *models.ParseTask, cfg.Workers.Stage2Parser.BufferSize),
		resultChan: make(chan *models.ParseResult, cfg.Workers.Stage2Parser.BufferSize),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the parser workers
func (p *Parser) Start() error {
	if !atomic.CompareAndSwapInt32(&p.running, 0, 1) {
		return fmt.Errorf("parser is already running")
	}

	logger.Infof("Starting %d parser workers", p.config.Workers.Stage2Parser.Count)

	// Start worker goroutines
	for i := 0; i < p.config.Workers.Stage2Parser.Count; i++ {
		p.wg.Add(1)
		go p.worker(i + 1)
	}

	return nil
}

// Stop stops the parser workers
func (p *Parser) Stop(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return nil // Already stopped
	}

	logger.Info("Stopping parser workers...")

	// Cancel context to signal workers to stop
	p.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All parser workers stopped gracefully")
	case <-time.After(timeout):
		logger.Warn("Parser workers stop timeout exceeded")
	}

	// Close channels
	close(p.taskChan)
	close(p.resultChan)

	return nil
}

// AddTask adds a parse task to the queue
func (p *Parser) AddTask(fetchResult *models.FetchResult) bool {
	if atomic.LoadInt32(&p.running) == 0 {
		return false
	}

	task := &models.ParseTask{
		FetchResult: fetchResult,
	}

	select {
	case p.taskChan <- task:
		return true
	default:
		return false // Channel is full
	}
}

// GetResultChannel returns the result channel for reading parse results
func (p *Parser) GetResultChannel() <-chan *models.ParseResult {
	return p.resultChan
}

// GetStatus returns the current status of the parser
func (p *Parser) GetStatus() models.StageStatus {
	p.stats.mu.RLock()
	defer p.stats.mu.RUnlock()

	return models.StageStatus{
		Name:             "parser",
		WorkersRunning:   p.config.Workers.Stage2Parser.Count,
		ActiveWorkers:    p.getActiveWorkerCount(),
		TaskBufferSize:   cap(p.taskChan),
		TaskBufferUsed:   len(p.taskChan),
		ResultBufferSize: cap(p.resultChan),
		ResultBufferUsed: len(p.resultChan),
		TotalProcessed:   p.stats.totalProcessed,
		TotalErrors:      p.stats.totalErrors,
		AvgProcessTime:   p.stats.avgProcessTime,
		LastActivity:     p.stats.lastActivity,
	}
}

// worker is the main worker goroutine that processes parse tasks
func (p *Parser) worker(workerID int) {
	defer p.wg.Done()

	logger.Debugf("Parser worker %d started", workerID)

	for {
		select {
		case <-p.ctx.Done():
			logger.Debugf("Parser worker %d stopping due to context cancellation", workerID)
			return
		case task, ok := <-p.taskChan:
			if !ok {
				logger.Debugf("Parser worker %d stopping due to closed task channel", workerID)
				return
			}

			if task == nil {
				continue
			}

			startTime := time.Now()
			result := p.processTask(workerID, task)
			duration := time.Since(startTime)
			result.Duration = duration

			// Update statistics
			p.updateStats(duration, result.Error != "")

			// Send result
			select {
			case p.resultChan <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// processTask processes a single parse task
func (p *Parser) processTask(workerID int, task *models.ParseTask) *models.ParseResult {
	logger.Debugf("Worker %d processing page %d", workerID, task.FetchResult.Page)

	if task.FetchResult.Error != "" || task.FetchResult.Data == nil {
		return &models.ParseResult{
			Page:  task.FetchResult.Page,
			CVEs:  []*models.ParsedCVE{},
			Error: task.FetchResult.Error,
		}
	}

	// Parse vulnerabilities from the data
	vulnerabilities, ok := task.FetchResult.Data["vulnerabilities"].([]interface{})
	if !ok {
		return &models.ParseResult{
			Page:  task.FetchResult.Page,
			CVEs:  []*models.ParsedCVE{},
			Error: "no vulnerabilities found in response",
		}
	}

	var parsedCVEs []*models.ParsedCVE
	var errors []string

	for _, vuln := range vulnerabilities {
		vulnMap, ok := vuln.(map[string]interface{})
		if !ok {
			errors = append(errors, "invalid vulnerability format")
			continue
		}

		parsedCVE, err := p.parseSingleCVE(vulnMap)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to parse CVE: %v", err))
			continue
		}

		if parsedCVE != nil {
			parsedCVEs = append(parsedCVEs, parsedCVE)
		}
	}

	result := &models.ParseResult{
		Page: task.FetchResult.Page,
		CVEs: parsedCVEs,
	}

	if len(errors) > 0 {
		result.Error = fmt.Sprintf("parsing errors: %s", strings.Join(errors, "; "))
	}

	logger.Debugf("Worker %d parsed %d CVEs from page %d", workerID, len(parsedCVEs), task.FetchResult.Page)
	return result
}

// parseSingleCVE parses a single CVE from the vulnerability data
func (p *Parser) parseSingleCVE(vulnData map[string]interface{}) (*models.ParsedCVE, error) {
	cveData, ok := vulnData["cve"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no CVE data found")
	}

	cveID, ok := cveData["id"].(string)
	if !ok || cveID == "" {
		return nil, fmt.Errorf("no CVE ID found")
	}

	// Extract year from CVE ID (e.g., CVE-2023-1234 -> 2023)
	parts := strings.Split(cveID, "-")
	year := "unknown"
	if len(parts) >= 2 {
		year = parts[1]
	}

	// Build processed CVE data
	processedData := p.buildProcessedCVE(cveData)

	return &models.ParsedCVE{
		CVEID: cveID,
		Year:  year,
		Data:  processedData,
	}, nil
}

// buildProcessedCVE builds the processed CVE data structure
func (p *Parser) buildProcessedCVE(cveData map[string]interface{}) map[string]interface{} {
	processed := make(map[string]interface{})

	// Basic fields
	if id, ok := cveData["id"].(string); ok {
		processed["id"] = id
	}
	processed["desc"] = p.getDescription(cveData)
	if published, ok := cveData["published"].(string); ok {
		processed["published"] = published
	}
	if lastModified, ok := cveData["lastModified"].(string); ok {
		processed["lastModified"] = lastModified
	}
	if vulnStatus, ok := cveData["vulnStatus"].(string); ok {
		processed["vulnStatus"] = vulnStatus
	}

	// Parse CVSS metrics
	metrics, _ := cveData["metrics"].(map[string]interface{})

	v4Metrics := p.parseCVSSv4(metrics)
	if v4Metrics != nil {
		processed["v4"] = v4Metrics
	}

	v3Metrics := p.parseCVSSv3(metrics)
	if v3Metrics != nil {
		processed["v3"] = v3Metrics
	}

	v2Metrics := p.parseCVSSv2(metrics)
	if v2Metrics != nil {
		processed["v2"] = v2Metrics
	}

	// Determine primary score
	primaryScore := p.getPrimaryScore(v4Metrics, v3Metrics, v2Metrics)
	if primaryScore != nil {
		processed["score"] = primaryScore["score"]
		processed["sev"] = primaryScore["sev"]
		processed["source"] = primaryScore["source"]
	}

	// Parse CISA KEV fields
	cisaData := p.parseCISAFields(cveData)
	processed["hasCisa"] = cisaData["hasCisa"]
	if cisaData["cisa"] != nil {
		processed["cisa"] = cisaData["cisa"]
	}

	return processed
}

// getDescription extracts the CVE description
func (p *Parser) getDescription(cveData map[string]interface{}) string {
	descriptions, ok := cveData["descriptions"].([]interface{})
	if !ok || len(descriptions) == 0 {
		return ""
	}

	firstDesc, ok := descriptions[0].(map[string]interface{})
	if !ok {
		return ""
	}

	value, ok := firstDesc["value"].(string)
	if !ok {
		return ""
	}

	return value
}

// parseCVSSv4 parses CVSS v4.0 metrics
func (p *Parser) parseCVSSv4(metrics map[string]interface{}) map[string]interface{} {
	if metrics == nil {
		return nil
	}

	cvssMetricV4, ok := metrics["cvssMetricV4"].([]interface{})
	if !ok || len(cvssMetricV4) == 0 {
		return nil
	}

	firstMetric, ok := cvssMetricV4[0].(map[string]interface{})
	if !ok {
		return nil
	}

	cvssData, ok := firstMetric["cvssData"].(map[string]interface{})
	if !ok {
		return nil
	}

	result := make(map[string]interface{})

	if score, ok := cvssData["baseScore"].(float64); ok {
		result["score"] = score
	}

	if sev, ok := firstMetric["baseSeverity"].(string); ok {
		result["sev"] = sev
	} else if sev, ok := cvssData["baseSeverity"].(string); ok {
		result["sev"] = sev
	}

	result["source"] = "v4.0"

	return result
}

// parseCVSSv3 parses CVSS v3.x metrics
func (p *Parser) parseCVSSv3(metrics map[string]interface{}) map[string]interface{} {
	if metrics == nil {
		return nil
	}

	// Try v3.1 first
	if result := p.extractCVSSv3Metrics(metrics, "cvssMetricV31", "v3.1"); result != nil {
		return result
	}

	// Try v3.0
	if result := p.extractCVSSv3Metrics(metrics, "cvssMetricV30", "v3.0"); result != nil {
		return result
	}

	return nil
}

// extractCVSSv3Metrics extracts CVSS v3 metrics for a specific version
func (p *Parser) extractCVSSv3Metrics(metrics map[string]interface{}, metricKey, version string) map[string]interface{} {
	cvssMetrics, ok := metrics[metricKey].([]interface{})
	if !ok || len(cvssMetrics) == 0 {
		return nil
	}

	firstMetric, ok := cvssMetrics[0].(map[string]interface{})
	if !ok {
		return nil
	}

	cvssData, ok := firstMetric["cvssData"].(map[string]interface{})
	if !ok {
		return nil
	}

	result := make(map[string]interface{})

	// Extract score
	if score, ok := cvssData["baseScore"].(float64); ok {
		result["score"] = score
	}

	// Extract severity (try multiple locations)
	if sev := p.extractSeverity(firstMetric, cvssData); sev != "" {
		result["sev"] = sev
	}

	result["source"] = version
	return result
}

// extractSeverity extracts severity from CVSS data
func (p *Parser) extractSeverity(firstMetric, cvssData map[string]interface{}) string {
	if sev, ok := firstMetric["baseSeverity"].(string); ok {
		return sev
	}
	if sev, ok := cvssData["baseSeverity"].(string); ok {
		return sev
	}
	return ""
}

// parseCVSSv2 parses CVSS v2.0 metrics
func (p *Parser) parseCVSSv2(metrics map[string]interface{}) map[string]interface{} {
	if metrics == nil {
		return nil
	}

	cvssMetricV2, ok := metrics["cvssMetricV2"].([]interface{})
	if !ok || len(cvssMetricV2) == 0 {
		return nil
	}

	firstMetric, ok := cvssMetricV2[0].(map[string]interface{})
	if !ok {
		return nil
	}

	cvssData, ok := firstMetric["cvssData"].(map[string]interface{})
	if !ok {
		return nil
	}

	result := make(map[string]interface{})

	if score, ok := cvssData["baseScore"].(float64); ok {
		result["score"] = score
	}

	if sev, ok := firstMetric["baseSeverity"].(string); ok {
		result["sev"] = sev
	} else if sev, ok := cvssData["baseSeverity"].(string); ok {
		result["sev"] = sev
	}

	result["source"] = "v2"

	return result
}

// getPrimaryScore determines the primary CVSS score (v4 > v3 > v2)
func (p *Parser) getPrimaryScore(v4, v3, v2 map[string]interface{}) map[string]interface{} {
	for _, metrics := range []map[string]interface{}{v4, v3, v2} {
		if metrics != nil {
			if score, ok := metrics["score"].(float64); ok && score > 0 {
				return metrics
			}
		}
	}
	return nil
}

// parseCISAFields parses CISA KEV fields
func (p *Parser) parseCISAFields(cveData map[string]interface{}) map[string]interface{} {
	cisaFields := []string{
		"cisaExploitAdd",
		"cisaActionDue",
		"cisaRequiredAction",
		"cisaVulnerabilityName",
	}

	cisaData := make(map[string]interface{})
	hasCisa := false

	for _, field := range cisaFields {
		if value, ok := cveData[field].(string); ok && value != "" {
			cisaData[field] = value
			hasCisa = true
		}
	}

	result := map[string]interface{}{
		"hasCisa": hasCisa,
	}

	if hasCisa {
		result["cisa"] = cisaData
	} else {
		result["cisa"] = nil
	}

	return result
}

// updateStats updates internal statistics
func (p *Parser) updateStats(duration time.Duration, hasError bool) {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()

	p.stats.totalProcessed++
	if hasError {
		p.stats.totalErrors++
	}

	// Update average process time
	if p.stats.avgProcessTime == 0 {
		p.stats.avgProcessTime = duration
	} else {
		p.stats.avgProcessTime = (p.stats.avgProcessTime + duration) / 2
	}

	p.stats.lastActivity = time.Now()
}

// getActiveWorkerCount returns the number of active workers (approximation)
func (p *Parser) getActiveWorkerCount() int {
	if atomic.LoadInt32(&p.running) == 0 {
		return 0
	}
	return p.config.Workers.Stage2Parser.Count
}
