package localcache

import (
	"container/list"
	"errors"
	"lb_localcache/buffer"
	"time"
)

var (
	// ErrEntryNotFound is an error type struct which is returned when entry was not found for provided key
	ErrEntryNotFound     = errors.New("Entry not found")
	ErrExpireTimeInvalid = errors.New("Entry expire time invalid")
)

const (
	segmentSizeBits          = 40
	maxSegmentSize    uint64 = 1 << segmentSizeBits
	segmentSize              = 32 * 1024 // 32kb
	defaultExpireTime        = 10 * time.Minute
)

type segment struct {
	hashmap   map[uint64]uint32
	entries   buffer.IBuffer
	clock     clock
	evictList *list.List
	stats     IStats
}

func newSegment() *segment {

}

func (s *segment) set(key string, hashKey uint64, value []byte, expiredTime time.Duration) error {
	if expiredTime < 0 {
		return ErrExpireTimeInvalid
	}
	expireAt := uint64(s.clock.Epoch(expiredTime))

	if preIndex, ok := s.hashmap[hashKey]; ok {
		if err := s.entries.Remove(int(preIndex)); err != nil {
			return err
		}
		delete(s.hashmap, hashKey)
	}

	entry := wrapEntry(expireAt, key, hashKey, value)

	// 当entries中满了时，需要逐一逐出缓存中元素直到能成功放下当前value
	for {
		index, err := s.entries.Push(entry)
		if err == nil {
			s.hashmap[hashKey] = uint32(index)
			s.evictList.PushFront(index)
			return nil
		}
		// 当前entries中已经满了，因此需要按照一定的**逐出策略**来逐出缓存
		ele := s.evictList.Back()
		// 移除entries对应缓存
		err = s.entries.Remove(ele.Value.(int))
		if err != nil {
			return err
		}
		// 移除FIFO链表中对应元素
		s.evictList.Remove(ele)
	}
}

func (s *segment) get(key string, hashKey uint64) ([]byte, error) {
	currentTimestamp := s.clock.TimeStamp()
	entry, err := s.getWarpEntry(key, hashKey)
	if err != nil {
		return nil, err
	}
	res := readEntry(entry)

	expiredTime := readExpireAtFromEntry(entry)
	if currentTimestamp > int64(expiredTime) {
		s.stats.miss()
		_ = s.entries.Remove(int(s.hashmap[hashKey]))
		delete(s.hashmap, hashKey)
		return nil, ErrEntryNotFound
	}
	s.stats.hit(key)
	return res, nil
}

func (s *segment) getWarpEntry(key string, hashKey uint64) ([]byte, error) {
	index, ok := s.hashmap[hashKey]
	if !ok {
		s.stats.miss()
		return nil, ErrEntryNotFound
	}

	entry, err := s.entries.Get(int(index))
	if err != nil {
		s.stats.miss()
		return nil, err
	}
	if entry == nil {
		s.stats.miss()
		return nil, ErrEntryNotFound
	}

	if entryKey := readKeyFromEntry(entry); key != entryKey {
		s.stats.collision()
		return nil, ErrEntryNotFound
	}
	return entry, nil
}

func (s *segment) Delete(hashKey uint64) error {
	index, ok := s.hashmap[hashKey]
	if !ok {
		s.stats.delMiss()
		return ErrEntryNotFound
	}
	if err := s.entries.Remove(int(index)); err != nil {
		return err
	}
	delete(s.hashmap, hashKey)
	s.stats.delHit()
	return nil
}

func (s *segment) len() int {
	res := len(s.hashmap)
	return res
}

func (s *segment) capacity() int {
	return s.entries.Capacity()
}

func (s *segment) cleanup(currentTimestamp int64) {
	indexs := s.entries.GetPlaceholderIndex()
	for index := range indexs {
		entry, err := s.entries.Get(index)
		if err != nil || entry == nil {
			continue
		}
		expireAt := int64(readExpireAtFromEntry(entry))
		if currentTimestamp-expireAt >= 0 {
			hash := readHashFromEntry(entry)
			delete(s.hashmap, hash)
			_ = s.entries.Remove(index)
			continue
		}
	}
}

func (s *segment) getStats() Stats {
	res := Stats{
		Hits:       s.stats.getHits(),
		Misses:     s.stats.getMisses(),
		DelHits:    s.stats.getDelHits(),
		DelMisses:  s.stats.getDelMisses(),
		Collisions: s.stats.getCollisions(),
	}
	return res
}

func (s *segment) getKeyHit(key string) int64 {
	return s.stats.getKeyHits(key)
}
