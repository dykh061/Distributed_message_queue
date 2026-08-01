package main

type Partition struct {
	partitionID uint16
	mq          Queue
}

func (p *Partition) init(id uint16) {
	p.partitionID = id
	p.mq.init()
}
