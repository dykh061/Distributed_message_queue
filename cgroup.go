package main

import (
	"bufio"
	"net"
	"sync"
)

type ConsumerGroup struct {
	mu        sync.RWMutex
	cgroupID  uint16
	consumers []Consumers       // lưu danh sách các consumer trong consumer group này
	offset    []PartitionOffset // lưu danh sách các offset của từng partition mà consumer group này đang subscribe
}

func (cg *ConsumerGroup) init(id uint16) {
	cg.cgroupID = id
	cg.consumers = make([]Consumers, 0)
	cg.offset = make([]PartitionOffset, 0)
}

type Consumers struct {
	ConsumerID       uint16
	stream_rw        *bufio.ReadWriter
	conn             net.Conn
	partitionsOffset []PartitionOffset // lưu danh sách các ID của các partition mà consumer này đang subscribe
}

type PartitionOffset struct {
	partitionID uint16
	offset      uint32
}

func (cg *ConsumerGroup) getOffset(partitionID uint16) uint32 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	if len(cg.offset) == 0 {
		return uint32(0)
	}
	for _, po := range cg.offset {
		if po.partitionID == partitionID {
			return po.offset
		}
	}
	return uint32(0)
}

func (cg *ConsumerGroup) commitOffset(partitionID uint16, offset uint32) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	for i, po := range cg.offset {
		if po.partitionID == partitionID {
			cg.offset[i].offset = offset
			return true
		}
	}
	cg.offset = append(cg.offset, PartitionOffset{
		partitionID: partitionID,
		offset:      offset,
	})
	return true
}
