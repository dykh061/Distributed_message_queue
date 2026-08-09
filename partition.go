package main

type Partition struct {
	partitionID uint16
	offset      uint32
	mq          Queue
}

func (p *Partition) init(id uint16) {
	p.partitionID = id
	p.offset = 0
	p.mq.init()
}
