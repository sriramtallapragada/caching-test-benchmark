package main

import (
	"caching-benchmark/benchmark"
	"caching-benchmark/implementations"
	"caching-benchmark/workload"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/redis/rueidis"
)

// Config holds the parameters for a single benchmark scenario.
type Config struct {
	Name           string
	NumOperations  int
	NumKeys        int
	ReadWriteRatio float64
	Concurrency    int
	ValueSizeBytes int
	ZipfS          float64
	ZipfV          float64
}

func main() {
	// Define the different benchmark scenarios
	testConfigs := []Config{
		{
			Name:           "Read-Heavy (90% Read, 64B Values)",
			NumOperations:  100000,
			NumKeys:        10000,
			ReadWriteRatio: 0.9,
			Concurrency:    64,
			ValueSizeBytes: 64,
			ZipfS:          1.01,
			ZipfV:          1,
		},
		{
			Name:           "Write-Heavy (50% Read, 64B Values)",
			NumOperations:  100000,
			NumKeys:        10000,
			ReadWriteRatio: 0.5,
			Concurrency:    64,
			ValueSizeBytes: 64,
			ZipfS:          1.01,
			ZipfV:          1,
		},
		{
			Name:           "Uniform Workload (Worst-Case, 90% Read)",
			NumOperations:  100000,
			NumKeys:        10000,
			ReadWriteRatio: 0.9,
			Concurrency:    64,
			ValueSizeBytes: 64,
			ZipfS:          0, // Zipf parameters are ignored for uniform
			ZipfV:          0,
		},
		{
			Name:           "Memory-Intensive (90% Read, 1KB Values)",
			NumOperations:  50000, // Reduced ops to keep test duration reasonable
			NumKeys:        10000,
			ReadWriteRatio: 0.9,
			Concurrency:    64,
			ValueSizeBytes: 1024,
			ZipfS:          1.01,
			ZipfV:          1,
		},
		{
			Name:           "Large Value Scenario (90% Read, 2MB Values)",
			NumOperations:  2000, // Drastically reduced ops due to large payload size
			NumKeys:        100,  // Reduced keys to keep data prep manageable
			ReadWriteRatio: 0.9,
			Concurrency:    64, // Reduced concurrency to avoid overwhelming network
			ValueSizeBytes: 2 * 1024 * 1024,
			ZipfS:          1.01,
			ZipfV:          1,
		},
		{
			Name:           "Write-Heavy & Large Value (50% Read, 1MB Values)",
			NumOperations:  2000,
			NumKeys:        100, // Reduced keys to keep data prep manageable
			ReadWriteRatio: 0.5,
			Concurrency:    64,
			ValueSizeBytes: 2 * 1024 * 1024,
			ZipfS:          1.01,
			ZipfV:          1,
		},
	}

	ctx := context.Background()
	allResults := make(map[string][]benchmark.Result)

	for _, cfg := range testConfigs {
		log.Println("==========================================================")
		log.Printf("--- Starting Scenario: %s ---", cfg.Name)
		log.Printf("Preparing benchmark with %d operations on %d keys.", cfg.NumOperations, cfg.NumKeys)
		log.Printf("Concurrency: %d, Read/Write Ratio: %.2f, Value Size: %dB", cfg.Concurrency, cfg.ReadWriteRatio, cfg.ValueSizeBytes)

		var w []workload.Operation
		if cfg.Name == "Uniform Workload (Worst-Case, 90% Read)" {
			w = workload.GenerateUniform(cfg.NumOperations, cfg.NumKeys, cfg.ReadWriteRatio)
		} else {
			w = workload.Generate(cfg.NumOperations, cfg.NumKeys, cfg.ReadWriteRatio, cfg.ZipfS, cfg.ZipfV)
		}

		// Estimate key count for rueidis based on a 1GB memory budget
		// This is a rough estimation and a weakness of the key-count approach.
		estimatedKeyCount := (1 << 30) / (cfg.ValueSizeBytes + 50) // 50 bytes overhead per key

		strategies := []benchmark.CachingStrategy{
			implementations.NewRueidisCSCStrategy(estimatedKeyCount),
			implementations.NewRistrettoPubSubStrategy(1 << 30), // 1GB memory budget
		}

		for _, s := range strategies {
			log.Printf("\n--- Running Strategy: %s ---", s.Name())
			if err := prepareData(ctx, cfg.NumKeys, cfg.ValueSizeBytes); err != nil {
				log.Fatalf("Failed to prepare data for strategy %s: %v", s.Name(), err)
			}

			runner := benchmark.NewRunner(s, w, cfg.Concurrency, cfg.ValueSizeBytes)
			result, err := runner.Run(ctx)
			if err != nil {
				log.Printf("Error running benchmark for strategy %s: %v", s.Name(), err)
				continue
			}
			allResults[cfg.Name] = append(allResults[cfg.Name], result)
		}
	}

	printFinalComparison(allResults)
}

func prepareData(ctx context.Context, numKeys, valueSizeBytes int) error {
	log.Println("Preparing datastore for benchmark...")
	// TODO: For very large data pre-population, consider a context with a longer timeout.
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Do(ctx, client.B().Flushall().Build()).Error(); err != nil {
		return fmt.Errorf("failed to flush datastore: %w", err)
	}

	log.Printf("Pre-populating with %d keys of size %dB...", numKeys, valueSizeBytes)
	cmds := make(rueidis.Commands, 0, numKeys)
	value := generateValue(valueSizeBytes)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		cmds = append(cmds, client.B().Set().Key(key).Value(value).Build())
	}

	for _, resp := range client.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	log.Println("Data preparation complete.")
	return nil
}

func generateValue(size int) string {
	b := make([]byte, size)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func printFinalComparison(allResults map[string][]benchmark.Result) {
	log.Println("\n\n--- Final Benchmark Comparison ---")

	for scenarioName, results := range allResults {
		log.Printf("\n--- Scenario: %s ---", scenarioName)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)
		fmt.Fprintln(w, "Strategy\tOps/sec\tHit Rate (%)\tAvg Latency (ms)\tP95 Latency (ms)\t")

		for _, r := range results {
			sort.Slice(r.Latencies, func(i, j int) bool {
				return r.Latencies[i] < r.Latencies[j]
			})

			var p95Latency time.Duration
			if len(r.Latencies) > 20 {
				p95Index := int(float64(len(r.Latencies)) * 0.95)
				p95Latency = r.Latencies[p95Index]
			}

			var totalLatency time.Duration
			for _, lat := range r.Latencies {
				totalLatency += lat
			}
			avgLatency := totalLatency / time.Duration(len(r.Latencies))

			fmt.Fprintf(w, "%s\t%.2f\t%.2f\t%.4f\t%.4f\t\n",
				r.StrategyName,
				r.OpsPerSecond,
				r.HitRate*100,
				float64(avgLatency.Microseconds())/1000.0,
				float64(p95Latency.Microseconds())/1000.0,
			)
		}
		w.Flush()
	}
}
