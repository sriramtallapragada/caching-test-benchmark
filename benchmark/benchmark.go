package benchmark

import (
	"caching-benchmark/workload"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Runner struct {
	strategy       CachingStrategy
	workload       []workload.Operation
	concurrency    int
	valueSizeBytes int
	result         Result
}

func NewRunner(strategy CachingStrategy, workload []workload.Operation, concurrency, valueSizeBytes int) *Runner {
	return &Runner{
		strategy:       strategy,
		workload:       workload,
		concurrency:    concurrency,
		valueSizeBytes: valueSizeBytes,
		result: Result{
			StrategyName: strategy.Name(),
			Latencies:    make([]time.Duration, 0, len(workload)),
		},
	}
}

func (r *Runner) Run(ctx context.Context) (Result, error) {
	log.Printf("Initializing strategy: %s", r.strategy.Name())
	if err := r.strategy.Init(ctx); err != nil {
		return r.result, fmt.Errorf("failed to initialize strategy: %w", err)
	}
	defer r.strategy.Close(ctx)

	var wg sync.WaitGroup
	wg.Add(r.concurrency)

	opsChan := make(chan workload.Operation, len(r.workload))
	for _, op := range r.workload {
		opsChan <- op
	}
	close(opsChan)

	latencyChan := make(chan time.Duration, len(r.workload))
	startTime := time.Now()

	log.Printf("Starting benchmark with %d concurrent workers...", r.concurrency)
	for i := 0; i < r.concurrency; i++ {
		go r.worker(ctx, &wg, opsChan, latencyChan)
	}

	wg.Wait()
	close(latencyChan)

	r.result.TotalDuration = time.Since(startTime)
	r.result.TotalOperations = int64(len(r.workload))

	for lat := range latencyChan {
		r.result.Latencies = append(r.result.Latencies, lat)
	}

	r.calculateFinalMetrics()
	r.printResults()

	return r.result, nil
}

func (r *Runner) worker(ctx context.Context, wg *sync.WaitGroup, ops <-chan workload.Operation, latencies chan<- time.Duration) {
	defer wg.Done()
	// Each worker generates its value once to avoid repeated allocation.
	valueToWrite := generateValue(r.valueSizeBytes)

	for op := range ops {
		var err error
		var hit bool
		var start time.Time

		start = time.Now()
		switch op.Type {
		case workload.ReadOp:
			_, hit, err = r.strategy.Read(ctx, op.Key)
			if err == nil {
				if hit {
					atomic.AddInt64(&r.result.TotalHits, 1)
				} else {
					atomic.AddInt64(&r.result.TotalMisses, 1)
				}
			}
		case workload.WriteOp:
			err = r.strategy.Write(ctx, op.Key, valueToWrite)
			if err == nil {
				atomic.AddInt64(&r.result.TotalWrites, 1)
			}
		}
		latency := time.Since(start)
		latencies <- latency

		if err != nil {
			atomic.AddInt64(&r.result.TotalErrors, 1)
		}
	}
}

func (r *Runner) calculateFinalMetrics() {
	if r.result.TotalHits+r.result.TotalMisses > 0 {
		r.result.HitRate = float64(r.result.TotalHits) / float64(r.result.TotalHits+r.result.TotalMisses)
	}
	if r.result.TotalDuration.Seconds() > 0 {
		r.result.OpsPerSecond = float64(r.result.TotalOperations) / r.result.TotalDuration.Seconds()
	}
}

func (r *Runner) printResults() {
	log.Println("--- Benchmark Results ---")
	log.Printf("Strategy: %s", r.result.StrategyName)
	log.Printf("Total Duration: %v", r.result.TotalDuration)
	log.Printf("Total Operations: %d", r.result.TotalOperations)
	log.Printf("Concurrency: %d", r.concurrency)
	log.Printf("Ops/sec: %.2f", r.result.OpsPerSecond)
	log.Printf("L1 Cache Hit Rate: %.2f%%", r.result.HitRate*100)
	log.Printf("Total Hits: %d", r.result.TotalHits)
	log.Printf("Total Misses: %d", r.result.TotalMisses)
	log.Printf("Total Writes: %d", r.result.TotalWrites)
	log.Printf("Total Errors: %d", r.result.TotalErrors)
	log.Println("-------------------------")
}

func generateValue(size int) string {
	b := make([]byte, size)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
