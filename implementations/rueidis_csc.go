package implementations

import (
	"caching-benchmark/benchmark"
	"context"
	"time"

	"github.com/redis/rueidis"
)

type RueidisCSCStrategy struct {
	client        rueidis.Client
	keyCountLimit int
}

func NewRueidisCSCStrategy(keyCountLimit int) benchmark.CachingStrategy {
	return &RueidisCSCStrategy{keyCountLimit: keyCountLimit}
}

func (s *RueidisCSCStrategy) Name() string {
	return "Rueidis Client-Side Caching"
}

func (s *RueidisCSCStrategy) Init(ctx context.Context) error {
	var err error
	s.client, err = rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{"127.0.0.1:6379"},
		CacheSizeEachConn: s.keyCountLimit,
	})
	return err
}

func (s *RueidisCSCStrategy) Read(ctx context.Context, key string) (value string, hit bool, err error) {
	// Use .Cache() to create a cacheable command and pass a time.Duration for the TTL.
	cacheableCmd := s.client.B().Get().Key(key).Cache()
	resp := s.client.DoCache(ctx, cacheableCmd, 10*time.Minute)

	err = resp.Error()
	if err == nil {
		value, err = resp.ToString()
	}

	// IsCacheHit() is a method on the RedisResult.
	return value, resp.IsCacheHit(), err
}

func (s *RueidisCSCStrategy) Write(ctx context.Context, key, value string) error {
	return s.client.Do(ctx, s.client.B().Set().Key(key).Value(value).Build()).Error()
}

func (s *RueidisCSCStrategy) Close(ctx context.Context) error {
	s.client.Close()
	return nil
}
