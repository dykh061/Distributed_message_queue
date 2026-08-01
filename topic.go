package main

type Topic struct {
	TopicID       uint16
	partitions    []Partition
	cgroups       []CGroup
	nextpartition uint32
}

func (t *Topic) init(id uint16) {
	t.TopicID = id
	t.partitions = make([]Partition, 3)
	for i := range t.partitions {
		t.partitions[i].init(uint16(i))
	}
	t.cgroups = make([]CGroup, 0)
	t.nextpartition = 0
}

func (t *Topic) selectNextPartition() *Partition {
	if len(t.partitions) == 0 {
		return nil
	}
	idx := t.nextpartition
	t.nextpartition = (t.nextpartition + 1) % uint32(len(t.partitions))
	return &t.partitions[idx]
}

func (t *Topic) rebalanceConsumerGroup(cgroupIDX int) error {
	cgroup := &t.cgroups[cgroupIDX]
	for i := range cgroup.consumers {
		cgroup.consumers[i].partitions = nil
	}
	for i := range t.partitions {
		partitionID := t.partitions[i].partitionID
		consumerIDX := i % len(cgroup.consumers)
		cgroup.consumers[consumerIDX].partitions = append(cgroup.consumers[consumerIDX].partitions, partitionID)
	}
	return nil
}
