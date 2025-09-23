package factory

import (
	"fmt"

	"github.com/luhtaf/nvd-fetcher/internal/config"
	"github.com/luhtaf/nvd-fetcher/internal/models"
)

// IndexerStrategy defines the interface for different indexing strategies
type IndexerStrategy interface {
	Name() string
	IndexDocuments(documents []*models.ESDocument) (*models.IndexResult, error)
	Close() error
}

// IndexerType represents the type of indexer strategy
type IndexerType string

const (
	// BulkIndexer uses standard bulk indexing
	BulkIndexer IndexerType = "bulk"

	// StreamingIndexer uses streaming bulk indexing for memory efficiency
	StreamingIndexer IndexerType = "streaming"

	// ParallelIndexer uses parallel bulk indexing for high throughput
	ParallelIndexer IndexerType = "parallel"
)

// Factory creates indexer strategies
type Factory struct {
	config *config.Config
}

// NewFactory creates a new indexer factory
func NewFactory(cfg *config.Config) *Factory {
	return &Factory{
		config: cfg,
	}
}

// CreateIndexer creates an indexer strategy based on the specified type
func (f *Factory) CreateIndexer(indexerType IndexerType) (IndexerStrategy, error) {
	switch indexerType {
	case BulkIndexer:
		return NewBulkIndexer(f.config)
	case StreamingIndexer:
		return NewStreamingIndexer(f.config)
	case ParallelIndexer:
		return NewParallelIndexer(f.config)
	default:
		return nil, fmt.Errorf("unknown indexer type: %s", indexerType)
	}
}

// GetAvailableTypes returns the list of available indexer types
func (f *Factory) GetAvailableTypes() []IndexerType {
	return []IndexerType{
		BulkIndexer,
		StreamingIndexer,
		ParallelIndexer,
	}
}

// ValidateType checks if the given indexer type is valid
func (f *Factory) ValidateType(indexerType IndexerType) bool {
	for _, t := range f.GetAvailableTypes() {
		if t == indexerType {
			return true
		}
	}
	return false
}
