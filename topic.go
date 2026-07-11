package main

type Topic struct {
	TopicID uint16
	mq      Queue
}

func (t *Topic) init(id uint16) {
	t.TopicID = id
	t.mq.init()
}
