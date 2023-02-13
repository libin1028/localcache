package localcache

import "time"

// options.go
type options struct {
	hashFunc       HashFunc
	bucketCount    uint64
	maxBytes       uint64
	cleanTime      time.Duration
	statsEnabled   bool
	cleanupEnabled bool
}

type Opt func(option *options)

func SetHashFunc(hashFunc HashFunc) Opt {
	return func(option *options) {
		option.hashFunc = hashFunc
	}
}

func SetBucketCount(bucketCount uint64) Opt {
	return func(option *options) {
		option.bucketCount = bucketCount
	}
}

func SetMaxBytes(maxBytes uint64) Opt {
	return func(option *options) {
		option.maxBytes = maxBytes
	}
}

func SetCleanTime(cleanTime time.Duration) Opt {
	return func(option *options) {
		option.cleanTime = cleanTime
	}
}

func SetStatsEnabled(statsEnabled bool) Opt {
	return func(option *options) {
		option.statsEnabled = statsEnabled
	}
}

func SetCleanupEnabled(cleanupEnabled bool) Opt {
	return func(option *options) {
		option.cleanupEnabled = cleanupEnabled
	}
}
