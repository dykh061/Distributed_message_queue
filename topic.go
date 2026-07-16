package main

type Topic struct {
	TopicID uint16
	mq      Queue
	cgroups []CGroup
}

func (t *Topic) init(id uint16) {
	t.TopicID = id
	t.mq.init()
	t.cgroups = make([]CGroup, 0)
}
