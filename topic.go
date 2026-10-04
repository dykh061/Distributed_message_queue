package main

import (
	"fmt"
	"sync"
)

type Topic struct {
	mu            sync.RWMutex
	topicID       uint16
	partitions    []*Partition
	cgroups       []*ConsumerGroup
	nextPartition uint32
	dataDir       string
}

func (t *Topic) init(id uint16, dataDir string) error {
	t.topicID = id
	t.dataDir = dataDir
	t.partitions = make([]*Partition, 3)
	for i := range t.partitions {
		if err := t.partitions[i].init(uint16(i), t.dataDir, t.topicID); err != nil {
			return fmt.Errorf("init partition %d for topic %d: %w", i, id, err)
		}
	}
	t.cgroups = make([]*ConsumerGroup, 0)
	t.nextPartition = 0
	return nil
}

func (t *Topic) selectNextPartition() *Partition {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.partitions) == 0 {
		return nil
	}
	idx := t.nextPartition
	t.nextPartition = (t.nextPartition + 1) % uint32(len(t.partitions))
	return t.partitions[idx]
}
