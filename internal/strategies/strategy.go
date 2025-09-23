package strategies

import (
	"context"
	"fmt"

	"github.com/luhtaf/nvd-fetcher/internal/config"
)

// Strategy defines the interface for different pipeline execution strategies
type Strategy interface {
	// Name returns the strategy name
	Name() string

	// Prepare setups the strategy before execution (date ranges, configs, etc.)
	Prepare(cfg *config.Config) error

	// Execute runs the strategy with the given context and config
	Execute(ctx context.Context, cfg *config.Config) error

	// Cleanup performs any necessary cleanup after execution
	Cleanup(cfg *config.Config) error

	// GetDescription returns a human-readable description of what this strategy does
	GetDescription() string
}

// StrategyFactory creates strategies based on mode
type StrategyFactory struct{}

// NewStrategyFactory creates a new strategy factory
func NewStrategyFactory() *StrategyFactory {
	return &StrategyFactory{}
}

// CreateStrategy returns the appropriate strategy for the given mode
func (sf *StrategyFactory) CreateStrategy(mode string) (Strategy, error) {
	switch mode {
	case "init":
		return NewInitStrategy(), nil
	case "update":
		return NewUpdateStrategy(), nil
	default:
		return nil, fmt.Errorf("unknown mode: %s. Available modes: init, update", mode)
	}
}

// GetAvailableStrategies returns a list of all available strategies
func (sf *StrategyFactory) GetAvailableStrategies() []string {
	return []string{"init", "update"}
}
