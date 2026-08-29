package main

import "sync"

type Topic struct {
	mu            sync.RWMutex
	topicID       uint16
	partitions    []*Partition
	cgroups       []*ConsumerGroup
	nextPartition uint32
}

func (t *Topic) init(id uint16) {
	t.topicID = id
	t.partitions = make([]*Partition, 3)
	for i := range t.partitions {
		t.partitions[i] = &Partition{}
		t.partitions[i].init(uint16(i))
	}
	t.cgroups = make([]*ConsumerGroup, 0)
	t.nextPartition = 0
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
