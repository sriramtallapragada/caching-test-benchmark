package workload

import (
	"fmt"
	"math/rand"
	"time"

	xrand "golang.org/x/exp/rand"
)

type OperationType int

const (
	ReadOp OperationType = iota
	WriteOp
)

type Operation struct {
	Type OperationType
	Key  string
}

// Generate generates a workload with a given number of operations and keys.
// readWriteRatio determines the proportion of reads to writes (e.g., 0.9 for 90% reads).
// zipfS and zipfV are parameters for the Zipf distribution, controlling the skew.
func Generate(numOps, numKeys int, readWriteRatio, zipfS, zipfV float64) []Operation {
	ops := make([]Operation, numOps)

	// Source and generator for Zipf distribution from x/exp/rand
	zipfSource := xrand.NewSource(uint64(time.Now().UnixNano()))
	zipfRng := xrand.New(zipfSource)
	zipf := xrand.NewZipf(zipfRng, zipfS, zipfV, uint64(numKeys-1))

	// Generator for read/write ratio from math/rand
	ratioRng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("key-%d", zipf.Uint64())
		opType := ReadOp
		if ratioRng.Float64() > readWriteRatio {
			opType = WriteOp
		}
		ops[i] = Operation{
			Type: opType,
			Key:  key,
		}
	}
	return ops
}

// GenerateUniform generates a workload where every key has an equal probability of being accessed.
// This represents a worst-case scenario for caching.
func GenerateUniform(numOps, numKeys int, readWriteRatio float64) []Operation {
	ops := make([]Operation, numOps)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(numKeys))
		opType := ReadOp
		if rng.Float64() > readWriteRatio {
			opType = WriteOp
		}
		ops[i] = Operation{
			Type: opType,
			Key:  key,
		}
	}
	return ops
}
