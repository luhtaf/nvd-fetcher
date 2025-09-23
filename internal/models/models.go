package models

import (
	"time"
)

// FetchTask represents a task for the fetcher stage
type FetchTask struct {
	Page       int `json:"page"`
	StartIndex int `json:"start_index"`
	PerPage    int `json:"per_page"`
}

// FetchResult represents the result from a fetch operation
type FetchResult struct {
	Page       int                    `json:"page"`
	Data       map[string]interface{} `json:"data,omitempty"`
	Error      string                 `json:"error,omitempty"`
	StatusCode int                    `json:"status_code,omitempty"`
	Duration   time.Duration          `json:"duration"`
}

// CVETask represents a single CVE processing task (NEW - True Concurrency Model)
type CVETask struct {
	CVEID     string                 `json:"cve_id"`
	Year      string                 `json:"year"`
	Page      int                    `json:"page"`
	Source    string                 `json:"source"` // "stage1", "stage2", "stage3"
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// ParseTask represents a task for the parser stage
type ParseTask struct {
	FetchResult *FetchResult `json:"fetch_result"`
}

// ParsedCVE represents a parsed CVE entry
type ParsedCVE struct {
	CVEID string                 `json:"cve_id"`
	Year  string                 `json:"year"`
	Data  map[string]interface{} `json:"data"`
}

// ParseResult represents the result from a parse operation
type ParseResult struct {
	Page     int           `json:"page"`
	CVEs     []*ParsedCVE  `json:"cves"`
	Error    string        `json:"error,omitempty"`
	Duration time.Duration `json:"duration"`
}

// IndexTask represents a task for the indexer stage
type IndexTask struct {
	ParseResult *ParseResult `json:"parse_result"`
}

// IndexResult represents the result from an index operation
type IndexResult struct {
	Page         int           `json:"page"`
	SuccessCount int           `json:"success_count"`
	FailedCount  int           `json:"failed_count"`
	Error        string        `json:"error,omitempty"`
	Duration     time.Duration `json:"duration"`
	Errors       []error       `json:"errors,omitempty"`
}

// CVSSMetrics represents CVSS metrics for a CVE
type CVSSMetrics struct {
	Score  *float64 `json:"score,omitempty"`
	Sev    *string  `json:"sev,omitempty"`
	Source *string  `json:"source,omitempty"`
}

// CISAData represents CISA KEV data
type CISAData struct {
	ExploitAdd        *string `json:"cisaExploitAdd,omitempty"`
	ActionDue         *string `json:"cisaActionDue,omitempty"`
	RequiredAction    *string `json:"cisaRequiredAction,omitempty"`
	VulnerabilityName *string `json:"cisaVulnerabilityName,omitempty"`
}

// ProcessedCVE represents a fully processed CVE
type ProcessedCVE struct {
	Description  string       `json:"desc"`
	Published    *string      `json:"published,omitempty"`
	LastModified *string      `json:"lastModified,omitempty"`
	VulnStatus   *string      `json:"vulnStatus,omitempty"`
	V4           *CVSSMetrics `json:"v4,omitempty"`
	V3           *CVSSMetrics `json:"v3,omitempty"`
	V2           *CVSSMetrics `json:"v2,omitempty"`
	Score        *float64     `json:"score,omitempty"`
	Sev          *string      `json:"sev,omitempty"`
	Source       *string      `json:"source,omitempty"`
	HasCISA      bool         `json:"hasCisa"`
	CISA         *CISAData    `json:"cisa,omitempty"`
}

// ESDocument represents an Elasticsearch document
type ESDocument struct {
	Index  string                 `json:"_index"`
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

// PipelineStats represents overall pipeline statistics
type PipelineStats struct {
	StartTime     time.Time  `json:"start_time"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	PagesFetched  int64      `json:"pages_fetched"`
	PagesParsed   int64      `json:"pages_parsed"`
	PagesIndexed  int64      `json:"pages_indexed"`
	TotalCVEs     int64      `json:"total_cves"`
	TotalResults  *int64     `json:"total_results,omitempty"`
	ErrorCount    int64      `json:"error_count"`
	SuccessRate   float64    `json:"success_rate"`
	CVEsPerSecond float64    `json:"cves_per_second"`
}

// StageStatus represents the status of a pipeline stage
type StageStatus struct {
	Name             string        `json:"name"`
	WorkersRunning   int           `json:"workers_running"`
	ActiveWorkers    int           `json:"active_workers"`
	TaskBufferSize   int           `json:"task_buffer_size"`
	TaskBufferUsed   int           `json:"task_buffer_used"`
	ResultBufferSize int           `json:"result_buffer_size"`
	ResultBufferUsed int           `json:"result_buffer_used"`
	TotalProcessed   int64         `json:"total_processed"`
	TotalErrors      int64         `json:"total_errors"`
	AvgProcessTime   time.Duration `json:"avg_process_time"`
	LastActivity     time.Time     `json:"last_activity"`
}

// PipelineStatus represents the overall pipeline status
type PipelineStatus struct {
	Running   bool                   `json:"running"`
	StartTime time.Time              `json:"start_time"`
	Uptime    time.Duration          `json:"uptime"`
	Stats     *PipelineStats         `json:"stats"`
	Stages    []StageStatus          `json:"stages"`
	RateLimit map[string]interface{} `json:"rate_limit,omitempty"`
}

// NVDResponse represents the structure of NVD API response
type NVDResponse struct {
	ResultsPerPage  int             `json:"resultsPerPage"`
	StartIndex      int             `json:"startIndex"`
	TotalResults    int             `json:"totalResults"`
	Format          string          `json:"format"`
	Version         string          `json:"version"`
	Timestamp       string          `json:"timestamp"`
	Vulnerabilities []Vulnerability `json:"vulnerabilities"`
}

// Vulnerability represents a single vulnerability from NVD
type Vulnerability struct {
	CVE CVEItem `json:"cve"`
}

// CVEItem represents the CVE item structure
type CVEItem struct {
	ID                    string                 `json:"id"`
	SourceIdentifier      string                 `json:"sourceIdentifier,omitempty"`
	Published             string                 `json:"published,omitempty"`
	LastModified          string                 `json:"lastModified,omitempty"`
	VulnStatus            string                 `json:"vulnStatus,omitempty"`
	Descriptions          []Description          `json:"descriptions,omitempty"`
	Metrics               map[string]interface{} `json:"metrics,omitempty"`
	CisaExploitAdd        string                 `json:"cisaExploitAdd,omitempty"`
	CisaActionDue         string                 `json:"cisaActionDue,omitempty"`
	CisaRequiredAction    string                 `json:"cisaRequiredAction,omitempty"`
	CisaVulnerabilityName string                 `json:"cisaVulnerabilityName,omitempty"`
}

// Description represents CVE description
type Description struct {
	Lang  string `json:"lang"`
	Value string `json:"value"`
}

// HealthCheck represents system health status
type HealthCheck struct {
	Status     string            `json:"status"`
	Timestamp  time.Time         `json:"timestamp"`
	Version    string            `json:"version"`
	Uptime     time.Duration     `json:"uptime"`
	Components map[string]string `json:"components"`
	Errors     []string          `json:"errors,omitempty"`
}
