package benchmark

import (
	"context"
	"time"
)

// CachingStrategy defines the interface for a caching implementation.
// This allows us to benchmark different strategies with the same test harness.
type CachingStrategy interface {
	// Name returns the name of the strategy.
	Name() string
	// Init initializes the strategy, preparing it for the benchmark.
	// This includes setting up clients, listeners, etc.
	Init(ctx context.Context) error
	// Read performs a read operation for a given key.
	// It should return the value and whether it was a cache hit.
	Read(ctx context.Context, key string) (value string, hit bool, err error)
	// Write performs a write operation for a given key and value.
	Write(ctx context.Context, key, value string) error
	// Close cleans up any resources used by the strategy.
	Close(ctx context.Context) error
}

// Result holds the collected metrics from a single benchmark run.
type Result struct {
	StrategyName    string
	TotalOperations int64
	TotalHits       int64
	TotalMisses     int64
	TotalWrites     int64
	TotalErrors     int64
	TotalDuration   time.Duration
	HitRate         float64
	OpsPerSecond    float64
	Latencies       []time.Duration
}
