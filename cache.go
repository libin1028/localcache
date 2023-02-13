package localcache

import (
	"errors"
	"lb_localcache/util"
	"sync"
	"time"
)

var (
	ErrShardCount = errors.New("shard count must be power of two")
	ErrBytes      = errors.New("maxBytes must be greater than 0")
)

const (
	defaultBucketCount    = 256
	defaultMaxBytes       = 512 * 1024 * 1024 // 512M
	defaultCleanTIme      = time.Minute * 10
	defaultStatsEnabled   = false
	defaultCleanupEnabled = false
)

type cache struct {
	// hashFunc represents used hash func
	hashFunc HashFunc
	// bucketCount represents the number of segments within a cache instance. value must be a power of two.
	bucketCount uint64
	// bucketMask is bitwise AND applied to the hashVal to find the segment id.
	bucketMask uint64
	// segment is shard
	segments []*segment
	// segment lock
	locks []sync.RWMutex
	// close cache
	close chan struct{}
}

// NewCache constructor cache instance
func NewCache(opts ...Opt) (ICache, error) {
	option := &options{
		hashFunc:       NewDefaultHashFunc(),
		bucketCount:    defaultBucketCount,
		maxBytes:       defaultMaxBytes,
		cleanTime:      defaultCleanTIme,
		statsEnabled:   defaultStatsEnabled,
		cleanupEnabled: defaultCleanupEnabled,
	}

	for _, opt := range opts {
		opt(option)
	}

	if !util.IsPowerOfTwo(option.bucketCount) {
		return nil, ErrShardCount
	}

	if option.maxBytes <= 0 {
		return nil, ErrBytes
	}

	segments := make([]*segment, option.bucketCount)
	locks := make([]sync.RWMutex, option.bucketCount)

	maxSegmentBytes := (option.maxBytes + option.bucketCount - 1) / option.bucketCount
	for index := range segments {
		segments[index] = newSegment(maxSegmentBytes, option.statsEnabled)
	}

	c := &cache{
		hashFunc:    option.hashFunc,
		bucketCount: option.bucketCount,
		bucketMask:  option.bucketCount - 1,
		segments:    segments,
		locks:       locks,
		close:       make(chan struct{}),
	}
	if option.cleanupEnabled {
		go c.cleanup(option.cleanTime)
	}
	return c, nil
}

func (c *cache) set(key string, value []byte) error {
	hashKey := c.hashFunc.Sum64(key)
	bucketIndex := hashKey & c.bucketMask
	c.locks[bucketIndex].Lock()
	defer c.locks[bucketIndex].Unlock()
	err := c.segments[bucketIndex].set(key, hashKey, value, defaultExpireTime)
	return err
}

func (c *cache) Get(key string) ([]byte, error) {
	hashKey := c.hashFunc.Sum64(key)
	bucketIndex := hashKey & c.bucketMask
	c.locks[bucketIndex].RLock()
	defer c.locks[bucketIndex].RUnlock()
	entry, err := c.segments[bucketIndex].get(key, hashKey)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (c *cache) SetWithExpiredTime(key string, value []byte, expiredTime time.Duration) error {
	hashKey := c.hashFunc.Sum64(key)
	bucketIndex := hashKey & c.bucketMask
	c.locks[bucketIndex].Lock()
	defer c.locks[bucketIndex].Unlock()
	err := c.segments[bucketIndex].set(key, hashKey, value, expiredTime)
	return err
}

func (c *cache) Delete(key string) error {
	hashKey := c.hashFunc.Sum64(key)
	bucketIndex := hashKey & c.bucketMask
	c.locks[bucketIndex].Lock()
	defer c.locks[bucketIndex].Unlock()
	err := c.segments[bucketIndex].Delete(hashKey)
	return err
}

func (c *cache) Len() int {
	cacheLen := 0
	for index := 0; index < int(c.bucketCount); index++ {
		c.locks[index].RLock()
		cacheLen += c.segments[index].len()
		c.locks[index].RUnlock()
	}
	return cacheLen
}

func (c *cache) Capacity() int {
	cacheCap := 0
	for index := 0; index < int(c.bucketCount); index++ {
		c.locks[index].RLock()
		cacheCap += c.segments[index].capacity()
		c.locks[index].RUnlock()
	}
	return cacheCap
}

func (c *cache) Close() error {
	close(c.close)
	return nil
}

func (c *cache) cleanup(cleanTime time.Duration) {
	ticker := time.NewTicker(cleanTime)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			for index := 0; index < int(c.bucketCount); index++ {
				c.locks[index].Lock()
				c.segments[index].cleanup(t.Unix())
				c.locks[index].Unlock()
			}
		case <-c.close:
			return
		}
	}
}

func (c *cache) Stats() Stats {
	s := Stats{}
	for _, shard := range c.segments {
		tmp := shard.getStats()
		s.Hits += tmp.Hits
		s.Misses += tmp.Misses
		s.DelHits += tmp.DelHits
		s.DelMisses += tmp.DelMisses
		s.Collisions += tmp.Collisions
	}
	return s
}

func (c *cache) GetKeyHit(key string) int64 {
	hashKey := c.hashFunc.Sum64(key)
	bucketIndex := hashKey & c.bucketMask
	c.locks[bucketIndex].Lock()
	defer c.locks[bucketIndex].Unlock()
	hit := c.segments[bucketIndex].getKeyHit(key)
	return hit
}
