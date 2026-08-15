package main

import (
	"bufio"
	"net"
)

type ConsumerGroup struct {
	cgroupID  uint16
	consumers []Consumers       // lưu danh sách các consumer trong consumer group này
	offset    []partitionOffset // Lưu các offset của từng partition đã được commit
}

func (cg *ConsumerGroup) init(id uint16) {
	cg.cgroupID = id
	cg.consumers = make([]Consumers, 0)
	cg.offset = make([]partitionOffset, 0)
}

type Consumers struct {
	ConsumerID       uint16
	stream_rw        *bufio.ReadWriter
	conn             net.Conn
	partitionsOffset []partitionOffset // lưu danh sách các ID của các partition mà consumer này đang subscribe
}

type PartitionOffset struct {
	partitionID uint16
	offset      uint32
}

func (cg *ConsumerGroup) getOffset(partitionID uint16) uint32 {
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

func (cg *ConsumerGroup) commitOffset(partitionID uint16, offset uint32) {
	for i, po := range cg.offset {
		if po.partitionID == partitionID {
			cg.offset[i].offset = offset
			return
		}
	}
	cg.offset = append(cg.offset, partitionOffset{
		partitionID: partitionID,
		offset:      offset,
	})

}
