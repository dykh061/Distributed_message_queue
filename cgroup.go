package main

import "net"

type CGroup struct {
	cgroupId  uint16
	offset    uint16
	consumers []Consumers
}

func (cg *CGroup) init(id uint16) {
	cg.cgroupId = id
	cg.offset = 0
	cg.consumers = make([]Consumers, 0)
}

type Consumers struct {
	status bool
	conn   net.Conn
}
