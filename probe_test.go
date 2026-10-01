package main

import (
	"testing"
)

func TestTemporaryMQCoreFlow(t *testing.T) {
	broker := &Broker{}
	broker.init()
	topic := &Topic{}
	topic.init(7)
	broker.topics = append(broker.topics, topic)

	for i := 0; i < 4; i++ {
		if _, err := broker.processProducerPCM([]byte{byte('a' + i)}, topic); err != nil {
			t.Fatal(err)
		}
	}
	got, _, found := topic.partitions[0].mq.fetchMessage(100, 0)
	if !found || string(got) != "\x00\x01a\x00\x01d" {
		t.Fatalf("partition 0 fetch = %q, found=%v", got, found)
	}
	got, _, found = topic.partitions[1].mq.fetchMessage(100, 0)
	if !found || string(got) != "\x00\x01b" {
		t.Fatalf("partition 1 fetch = %q, found=%v", got, found)
	}
	if got := topic.partitions[0].mq.getCount(); got != 2 {
		t.Fatalf("partition 0 count = %d, want 2", got)
	}

	group := &ConsumerGroup{}
	group.init(9)
	group.consumers = []*Consumers{{ConsumerID: 1}, {ConsumerID: 2}}
	if err := group.rebalanceConsumerGroup(topic.partitions); err != nil {
		t.Fatal(err)
	}
	if len(group.consumers[0].partitionsOffset) != 2 || len(group.consumers[1].partitionsOffset) != 1 {
		t.Fatalf("rebalance assignments = %d and %d, want 2 and 1", len(group.consumers[0].partitionsOffset), len(group.consumers[1].partitionsOffset))
	}

	echo := "ping"
	response, err := broker.processBrokerMessage(&Message{ECHO: &echo})
	if err != nil || response == nil || response.RESPONSE_ECHO == nil || *response.RESPONSE_ECHO != "I have receive : ping" {
		t.Fatalf("echo response = %#v, err=%v", response, err)
	}
}