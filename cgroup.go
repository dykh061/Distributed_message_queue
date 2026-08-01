package main

import "net"

type CGroup struct {
	cgroupId  uint16
	offsets   []partitionOffset // lưu offset của từng partition mà consumer group này đang subscribe
	consumers []Consumers       // lưu danh sách các consumer trong consumer group này
}
type partitionOffset struct {
	partitionID uint16
	offset      uint32
}

func (cg *CGroup) init(id uint16) {
	cg.cgroupId = id
	cg.offsets = make([]partitionOffset, 0)
	cg.consumers = make([]Consumers, 0)
}

type Consumers struct {
	ConsumerID uint16
	conn       net.Conn
	partitions []uint16 // lưu danh sách các ID của các partition mà consumer này đang subscribe
}
