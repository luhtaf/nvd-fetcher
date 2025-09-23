package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the NVD pipeline
type Config struct {
	NVD           NVDConfig           `mapstructure:"nvd"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	Workers       WorkersConfig       `mapstructure:"workers"`
	General       GeneralConfig       `mapstructure:"general"`
	Shutdown      ShutdownConfig      `mapstructure:"shutdown"`
}

// NVDConfig holds NVD API configuration
type NVDConfig struct {
	API      APIConfig `mapstructure:"api"`
	MaxPages int       `mapstructure:"max_pages"`
}

// APIConfig holds API-specific configuration
type APIConfig struct {
	BaseURL          string          `mapstructure:"base_url"`
	APIKey           string          `mapstructure:"api_key"`
	RateLimit        RateLimitConfig `mapstructure:"rate_limit"`
	Timeout          int             `mapstructure:"timeout"`
	PerPage          int             `mapstructure:"per_page"`
	RetryAttempts    int             `mapstructure:"retry_attempts"`
	RetryDelay       time.Duration   `mapstructure:"retry_delay"`
	LastModStartDate string          `mapstructure:"-"` // Runtime field for update mode
	LastModEndDate   string          `mapstructure:"-"` // Runtime field for update mode
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	RequestsPerSecond float64 `mapstructure:"requests_per_second"`
	BurstSize         int     `mapstructure:"burst_size"`
}

// ElasticsearchConfig holds Elasticsearch configuration
type ElasticsearchConfig struct {
	URL           string `mapstructure:"url"`
	VerifyCerts   bool   `mapstructure:"verify_certs"`
	Timeout       int    `mapstructure:"timeout"`
	IndexTemplate string `mapstructure:"index_template"`
	MaxRetries    int    `mapstructure:"max_retries"`
}

// WorkersConfig holds worker configuration for all stages
type WorkersConfig struct {
	Stage1Fetcher Stage1Config `mapstructure:"stage1_fetcher"`
	Stage2Parser  Stage2Config `mapstructure:"stage2_parser"`
	Stage3Indexer Stage3Config `mapstructure:"stage3_indexer"`
}

// Stage1Config holds fetcher stage configuration
type Stage1Config struct {
	Count      int `mapstructure:"count"`
	BufferSize int `mapstructure:"buffer_size"`
}

// Stage2Config holds parser stage configuration
type Stage2Config struct {
	Count      int `mapstructure:"count"`
	BufferSize int `mapstructure:"buffer_size"`
}

// Stage3Config holds indexer stage configuration
type Stage3Config struct {
	Count         int           `mapstructure:"count"`
	BufferSize    int           `mapstructure:"buffer_size"`
	BulkSize      int           `mapstructure:"bulk_size"`
	BulkTimeout   time.Duration `mapstructure:"bulk_timeout"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	Strategy      string        `mapstructure:"strategy"`
}

// GeneralConfig holds general application configuration
type GeneralConfig struct {
	Mode            string `mapstructure:"mode"` // "init" or "update"
	MaxPages        int    `mapstructure:"max_pages"`
	LogLevel        string `mapstructure:"log_level"`
	LogFormat       string `mapstructure:"log_format"`
	MetricsEnabled  bool   `mapstructure:"metrics_enabled"`
	MetricsPort     int    `mapstructure:"metrics_port"`
	LastRunFile     string `mapstructure:"last_run_file"`    // File to store last run timestamp
	DefaultLookback string `mapstructure:"default_lookback"` // Default lookback period for first run
}

// ShutdownConfig holds shutdown configuration
type ShutdownConfig struct {
	Timeout      time.Duration `mapstructure:"timeout"`
	ForceTimeout time.Duration `mapstructure:"force_timeout"`
}

// LoadConfig loads configuration from file
func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	// Set defaults
	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// NVD API defaults
	viper.SetDefault("nvd.api.base_url", "https://services.nvd.nist.gov/rest/json/cves/2.0")
	viper.SetDefault("nvd.api.rate_limit.requests_per_second", 10.0)
	viper.SetDefault("nvd.api.rate_limit.burst_size", 20)
	viper.SetDefault("nvd.api.timeout", 30)
	viper.SetDefault("nvd.api.per_page", 2000)
	viper.SetDefault("nvd.api.retry_attempts", 3)
	viper.SetDefault("nvd.api.retry_delay", "2s")

	// Elasticsearch defaults
	viper.SetDefault("elasticsearch.url", "http://localhost:9200")
	viper.SetDefault("elasticsearch.verify_certs", false)
	viper.SetDefault("elasticsearch.timeout", 60)
	viper.SetDefault("elasticsearch.index_template", "list-cve-{year}")
	viper.SetDefault("elasticsearch.max_retries", 3)

	// Workers defaults
	viper.SetDefault("workers.stage1_fetcher.count", 5)
	viper.SetDefault("workers.stage1_fetcher.buffer_size", 200)
	viper.SetDefault("workers.stage2_parser.count", 10)
	viper.SetDefault("workers.stage2_parser.buffer_size", 500)
	viper.SetDefault("workers.stage3_indexer.count", 3)
	viper.SetDefault("workers.stage3_indexer.buffer_size", 100)
	viper.SetDefault("workers.stage3_indexer.bulk_size", 2000)
	viper.SetDefault("workers.stage3_indexer.bulk_timeout", "30s")
	viper.SetDefault("workers.stage3_indexer.flush_interval", "10s")
	viper.SetDefault("workers.stage3_indexer.strategy", "bulk")

	// General defaults
	viper.SetDefault("general.max_pages", 0)
	viper.SetDefault("general.log_level", "info")
	viper.SetDefault("general.log_format", "json")
	viper.SetDefault("general.metrics_enabled", true)
	viper.SetDefault("general.metrics_port", 8080)

	// Shutdown defaults
	viper.SetDefault("shutdown.timeout", "60s")
	viper.SetDefault("shutdown.force_timeout", "10s")
}

// validateConfig validates the loaded configuration
func validateConfig(config *Config) error {
	if config.NVD.API.BaseURL == "" {
		return fmt.Errorf("nvd.api.base_url cannot be empty")
	}

	if config.NVD.API.RateLimit.RequestsPerSecond <= 0 {
		return fmt.Errorf("nvd.api.rate_limit.requests_per_second must be positive")
	}

	if config.NVD.API.RateLimit.BurstSize <= 0 {
		return fmt.Errorf("nvd.api.rate_limit.burst_size must be positive")
	}

	if config.Elasticsearch.URL == "" {
		return fmt.Errorf("elasticsearch.url cannot be empty")
	}

	if config.Workers.Stage1Fetcher.Count <= 0 {
		return fmt.Errorf("workers.stage1_fetcher.count must be positive")
	}

	if config.Workers.Stage2Parser.Count <= 0 {
		return fmt.Errorf("workers.stage2_parser.count must be positive")
	}

	if config.Workers.Stage3Indexer.Count <= 0 {
		return fmt.Errorf("workers.stage3_indexer.count must be positive")
	}

	if config.Workers.Stage3Indexer.BulkSize <= 0 {
		return fmt.Errorf("workers.stage3_indexer.bulk_size must be positive")
	}

	return nil
}
