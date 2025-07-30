package implementations

import (
	"caching-benchmark/benchmark"
	"context"
	"encoding/json"
	"log"

	"github.com/dgraph-io/ristretto"
	"github.com/redis/rueidis"
)

const InvalidationChannel = "cache-invalidation"

type RistrettoPubSubStrategy struct {
	l1Cache       *ristretto.Cache
	redisClient   rueidis.Client
	pubsubClient  rueidis.Client
	cancelBgTasks context.CancelFunc
	maxCost       int64
}

type InvalidationMessage struct {
	Key string `json:"key"`
}

func NewRistrettoPubSubStrategy(maxCost int64) benchmark.CachingStrategy {
	return &RistrettoPubSubStrategy{maxCost: maxCost}
}

func (s *RistrettoPubSubStrategy) Name() string {
	return "Ristretto L1 + Redis Pub/Sub"
}

func (s *RistrettoPubSubStrategy) Init(ctx context.Context) error {
	var err error
	// 1. Initialize Ristretto Cache
	s.l1Cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e6,
		MaxCost:     s.maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return err
	}

	// 2. Initialize Redis clients
	s.redisClient, err = rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		return err
	}
	s.pubsubClient, err = rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		return err
	}

	// 3. Start background listener
	bgCtx, cancel := context.WithCancel(context.Background())
	s.cancelBgTasks = cancel
	go s.listenForInvalidations(bgCtx)

	return nil
}

func (s *RistrettoPubSubStrategy) Read(ctx context.Context, key string) (value string, hit bool, err error) {
	if val, found := s.l1Cache.Get(key); found {
		return val.(string), true, nil
	}

	// L1 miss, get from L2
	value, err = s.redisClient.Do(ctx, s.redisClient.B().Get().Key(key).Build()).ToString()
	if err == nil {
		// Populate L1 cache
		s.l1Cache.Set(key, value, int64(len(value)))
	}
	return value, false, err
}

func (s *RistrettoPubSubStrategy) Write(ctx context.Context, key, value string) error {
	// 1. Set the value in Redis
	err := s.redisClient.Do(ctx, s.redisClient.B().Set().Key(key).Value(value).Build()).Error()
	if err != nil {
		return err
	}

	// 2. Publish invalidation message
	msg, _ := json.Marshal(InvalidationMessage{Key: key})
	return s.redisClient.Do(ctx, s.redisClient.B().Publish().Channel(InvalidationChannel).Message(string(msg)).Build()).Error()
}

func (s *RistrettoPubSubStrategy) Close(ctx context.Context) error {
	s.cancelBgTasks()
	s.l1Cache.Close()
	s.redisClient.Close()
	s.pubsubClient.Close()
	return nil
}

func (s *RistrettoPubSubStrategy) listenForInvalidations(ctx context.Context) {
	err := s.pubsubClient.Receive(ctx, s.pubsubClient.B().Subscribe().Channel(InvalidationChannel).Build(), func(msg rueidis.PubSubMessage) {
		var invalMsg InvalidationMessage
		if err := json.Unmarshal([]byte(msg.Message), &invalMsg); err == nil {
			if invalMsg.Key != "" {
				s.l1Cache.Del(invalMsg.Key)
			}
		}
	})
	if err != nil && err != context.Canceled {
		log.Printf("Error in Pub/Sub listener: %v", err)
	}
}
