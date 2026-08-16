package main

import "sync"

type Topic struct {
	mu            sync.RWMutex
	topicID       uint16
	partitions    []Partition
	cgroups       []ConsumerGroup
	nextPartition uint32
}

func (t *Topic) init(id uint16) {
	t.topicID = id
	t.partitions = make([]Partition, 3)
	for i := range t.partitions {
		t.partitions[i].init(uint16(i))
	}
	t.cgroups = make([]ConsumerGroup, 0)
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
	return &t.partitions[idx]
}

func (t *Topic) rebalanceConsumerGroup(cgroupIDX int) error {
	if cgroupIDX < 0 || cgroupIDX >= len(t.cgroups) {
		return nil
	}
	cgroup := &t.cgroups[cgroupIDX]
	if len(cgroup.consumers) == 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range cgroup.consumers {
		cgroup.consumers[i].partitionsOffset = nil
	}
	for i := range t.partitions {
		partitionID := t.partitions[i].partitionID
		consumerIDX := i % len(cgroup.consumers)
		cgroup.consumers[consumerIDX].partitionsOffset = append(cgroup.consumers[consumerIDX].partitionsOffset, PartitionOffset{
			partitionID: partitionID,
			offset:      cgroup.getOffset(partitionID),
		})
	}
	return nil
}
