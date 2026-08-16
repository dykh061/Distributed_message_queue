package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

const BROKER_PORT = 10000

type Broker struct {
	mu     sync.Mutex
	topics []Topic
}

func (broker *Broker) init() {
	broker.topics = make([]Topic, 0)
}

func (broker *Broker) printState(topicIdx int) {
	if topicIdx < 0 || topicIdx >= len(broker.topics) {
		return
	}
	fmt.Println("\n================ BROKER STATE ================")
	topic := broker.topics[topicIdx]
	fmt.Printf("TOPIC: %d\n\n", topic.topicID)
	for _, partition := range topic.partitions {
		fmt.Printf("P%d\n  Messages: %d\n\n", partition.partitionID, partition.mq.count)
	}
	if len(topic.cgroups) == 0 {
		fmt.Println("================================================")
		return
	}
	fmt.Printf("CONSUMER GROUP: %d\n\n", topic.cgroups[0].cgroupID)
	for _, cg := range topic.cgroups {
		fmt.Printf("Consumer %d\n", cg.cgroupID)
		for _, consumer := range cg.consumers {
			assignment := make([]string, 0, len(consumer.partitionsOffset))
			for _, p := range consumer.partitionsOffset {
				assignment = append(assignment, fmt.Sprintf("P%d", p.partitionID))
			}
			fmt.Printf("  Assignment: %s\n", strings.Join(assignment, ", "))
		}
		for _, po := range cg.offset {
			fmt.Printf("  P%d -> Next: %d | Commit: %d\n", po.partitionID, po.offset, po.offset)
		}
		fmt.Println()
	}
	fmt.Println("================================================")
}

// Khởi động server broker và lắng nghe các kết nối đến port
// khi có kết nối đến thì đồng ý kết nối và đọc dữ liệu từ stream
// sau đó parse dữ liệu và xữ lý dữ liệu cuối cùng gửi lại phản hồi về
func (broker *Broker) StartBrokerServer() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return err
	}
	defer l.Close()
	fmt.Println("========== BROKER START ==========")
	fmt.Printf("[Broker] Listening on :%d\n", BROKER_PORT)
	fmt.Println("[Broker] Topics initialized")
	fmt.Println()
	for {

		conn, err := l.Accept()
		if err != nil {
			return err
		}
		defer func() {
			err := conn.Close()
			if err != nil {
				log.Println(err)
			}
		}()
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		parsed_data, err := ReadMessageFromStream(stream_rw)
		if err == nil && parsed_data != nil {
			resp, err := broker.processBrokerMessage(parsed_data)
			if err != nil {
				return err
			}

			// fmt.Printf("Data from client: %s\n", *resp.RESPONSE_ECHO)
			err = WriteMessageToStream(stream_rw, resp)
			if err != nil {
				return err
			}
		}
	}
}

// sau khi parse dữ liệu từ stream sẽ có 1 message được tạo ra và dựa vào message này sẽ xữ lý dữ liệu và trả về phản hồi cho client
// Nếu nếu message có thuộc tính ECHO thì sẽ gọi hàm processEchoMessage để xữ lý dữ liệu và trả về phản hồi cho client
// Nếu nếu message có thuộc tính PRODUCER_REGISTER thì sẽ gọi hàm processProducerRegisterMessage để xữ lý dữ liệu và trả về phản hồi cho client
func (broker *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		resp, err := broker.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}
		return &Message{RESPONSE_ECHO: &resp}, nil
	}
	if message.PRODUCER_REGISTER != nil {
		resp, err := broker.processProducerRegisterMessage(message.PRODUCER_REGISTER)
		if err != nil {
			return nil, err
		}
		return &Message{RESPONSE_PRODUCER_REGISTER: resp}, nil
	}
	if message.CONSUMER_REGISTER != nil {
		resp, err := broker.processConsumerGroupConsump(message.CONSUMER_REGISTER)
		if err != nil {
			return nil, err
		}
		return &Message{RESPONSE_CONSUMER_REGISTER: resp}, nil
	}
	return nil, nil
}

func (broker *Broker) processProducerPCM(pcm_message []byte, idx int) (*byte, error) {
	fmt.Printf("[Broker] PCM\n  Producer: :%d\n  Topic: %d\n", broker.topics[idx].topicID, broker.topics[idx].topicID)
	partition := broker.topics[idx].selectNextPartition()
	if partition == nil {
		err := errors.New("No partition available for topic")
		return nil, err
	}
	partition.mq.push(pcm_message)
	fmt.Printf("[Broker] Route message\n  %s → Partition %d\n", strings.TrimSpace(string(pcm_message)), partition.partitionID)
	partition.mq.debug()
	var ack byte = 0
	return &ack, nil
}

func (broker *Broker) processEchoMessage(echo_message *string) (string, error) {
	return fmt.Sprintf("I have receive : %s", *echo_message), nil
}

func (broker *Broker) processConsumerGroupConsump(consumer_register_message *ConsumerRegister) (*byte, error) {
	fmt.Println("[Broker] New connection received")
	fmt.Printf("[Broker] Registering Consumer on :%d\n", consumer_register_message.port)
	fmt.Printf("[Broker] CONSUMER_REGISTER\n  Consumer: :%d\n  Group: %d\n  Port: %d\n", consumer_register_message.port, consumer_register_message.groupID, consumer_register_message.port)
	var topic_idx int = -1
	if len(broker.topics) == 0 {
		ntopic := &Topic{}
		ntopic.init(consumer_register_message.topicID)
		broker.topics = append(broker.topics, *ntopic)
		topic_idx = len(broker.topics) - 1
	} else {
		for index, tp := range broker.topics {
			if tp.topicID == consumer_register_message.topicID {
				topic_idx = index
				break
			}
		}
		if topic_idx == -1 {
			ntopic := &Topic{}
			ntopic.init(consumer_register_message.topicID)
			broker.topics = append(broker.topics, *ntopic)
			topic_idx = len(broker.topics) - 1
		}
	}
	var cgroup_idx int = -1
	if len(broker.topics[topic_idx].cgroups) == 0 {
		ncgroup := &ConsumerGroup{}
		ncgroup.init(consumer_register_message.groupID)
		broker.topics[topic_idx].cgroups = append(broker.topics[topic_idx].cgroups, *ncgroup)
		cgroup_idx = len(broker.topics[topic_idx].cgroups) - 1
	} else {
		for index, cg := range broker.topics[topic_idx].cgroups {
			if cg.cgroupID == consumer_register_message.groupID {
				cgroup_idx = index
				break
			}
		}
		if cgroup_idx == -1 {
			ncgroup := &ConsumerGroup{}
			ncgroup.init(consumer_register_message.groupID)
			broker.topics[topic_idx].cgroups = append(broker.topics[topic_idx].cgroups, *ncgroup)
			cgroup_idx = len(broker.topics[topic_idx].cgroups) - 1
		}
	}
	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", consumer_register_message.port))
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		broker.mu.Lock()
		broker.topics[topic_idx].cgroups[cgroup_idx].consumers = append(
			broker.topics[topic_idx].cgroups[cgroup_idx].consumers,
			Consumers{
				conn:       conn,
				stream_rw:  stream_rw,
				ConsumerID: uint16(len(broker.topics[topic_idx].cgroups[cgroup_idx].consumers)),
			},
		)
		fmt.Printf("[Broker] Connecting to Consumer :%d\n", consumer_register_message.port)
		fmt.Printf("[Broker] Consumer connected\n")
		broker.mu.Unlock()
		broker.topics[topic_idx].rebalanceConsumerGroup(cgroup_idx)
		broker.sendAssignmentToConsumerGroup(topic_idx, cgroup_idx)
		broker.printState(topic_idx)
		err = broker.handleConsumerConnection(stream_rw, topic_idx, cgroup_idx)
		if err != nil {
			panic(err)
		}
	}()
	var resp byte = 0
	return &resp, nil

	// giờ mình cần phải làm sao để gửi assiment cho từng consumer biết mỗi khi rebalance xong thì nó tự pull log về mà đọc
	// sau đó nó commit offset về cho broker biết là nó đã đọc đến đâu rồi

}

func (broker *Broker) handleConsumerConnection(stream_rw *bufio.ReadWriter, topic_idx, cgroup_idx int) error {
	for {
		resp, err := ReadMessageFromStream(stream_rw)
		if err != nil {
			panic(err)
		}
		if resp != nil {
			if resp.ASSIGNMENT_ACK != nil {
				fmt.Println("[Broker] Consumer assignment acknowledged")
			}
			if resp.COMMIT_OFFSET != nil {
				CommitOffset := resp.COMMIT_OFFSET
				partitionID := CommitOffset.PartitionID
				fmt.Printf("[Broker] COMMIT_OFFSET received\n  Group: %d\n  Partition: P%d\n  Offset: %d\n", CommitOffset.GroupID, partitionID, CommitOffset.Offset)
				if CommitOffset.GroupID != broker.topics[topic_idx].cgroups[cgroup_idx].cgroupID ||
					CommitOffset.TopicID != broker.topics[topic_idx].topicID {
					return errors.New("Commit offset message has mismatched group or topic ID")
				}
				success := broker.topics[topic_idx].cgroups[cgroup_idx].commitOffset(partitionID, CommitOffset.Offset)
				fmt.Println("[Broker] Updating committed offset")
				fmt.Printf("  group-%d\n    P%d: %d → %d\n", CommitOffset.GroupID, partitionID, broker.topics[topic_idx].cgroups[cgroup_idx].getOffset(partitionID), CommitOffset.Offset)
				var ack = CommitOffsetAck{
					partitionID: partitionID,
					offset:      CommitOffset.Offset,
					success:     success,
				}
				Message := &Message{
					COMMIT_OFFSET_ACK: &ack,
				}
				fmt.Printf("[Broker] COMMIT_OFFSET_ACK\n  Consumer: %d\n  Partition: P%d\n  CommittedOffset: %d\n", CommitOffset.GroupID, partitionID, CommitOffset.Offset)
				err := WriteSerializableToStream(stream_rw, COMMIT_OFFSET_ACK, Message.COMMIT_OFFSET_ACK)
				if err != nil {
					return err
				}
				broker.printState(topic_idx)
			}
			if resp.FETCH != nil {
				partitionID := resp.FETCH.partitionID
				fmt.Printf("[Broker] FETCH received\n  Consumer: %d\n  Group: %d\n  Partition: P%d\n  Offset: %d\n", cgroup_idx, broker.topics[topic_idx].cgroups[cgroup_idx].cgroupID, partitionID, resp.FETCH.offset)
				var partitionIDX int = -1
				for i := range broker.topics[topic_idx].partitions {
					if broker.topics[topic_idx].partitions[i].partitionID == partitionID {
						partitionIDX = i
						break
					}
				}
				if partitionIDX != -1 {
					messages, nextOffset, found := broker.topics[topic_idx].partitions[partitionIDX].mq.fetchMessage(uint16(100), resp.FETCH.offset)
					if !found {
						fmt.Printf("[Broker] No messages available\n")
					} else {
						messageCount := int(nextOffset - resp.FETCH.offset)
						fmt.Printf("[Broker] Reading P%d\n  Available messages: %d\n", partitionID, messageCount)
						fmt.Printf("[Broker] Sending FETCH_ACK\n  Consumer: %d\n  Partition: P%d\n  StartOffset: %d\n  MessageCount: %d\n  NextOffset: %d\n", cgroup_idx, partitionID, resp.FETCH.offset, messageCount, nextOffset)
					}
					fetch_ack := &Message{
						FETCH_ACK: &FetchAck{
							partitionID: broker.topics[topic_idx].partitions[partitionIDX].partitionID,
							found:       found,
							nextOffset:  nextOffset,
							data:        messages,
						},
					}
					err = WriteMessageToStream(stream_rw, fetch_ack)
					if err != nil {
						fmt.Printf("Error writing fetch ack to stream: %v\n", err)
					}
				}
			}
		}
	}
}

func (broker *Broker) sendAssignmentToConsumerGroup(topic_idx, cgroup_idx int) error {
	var err error
	group := &broker.topics[topic_idx].cgroups[cgroup_idx]
	fmt.Println("========== REBALANCE ==========")
	fmt.Printf("Group: %d\nConsumers: %d\nPartitions: %d\n\n", group.cgroupID, len(group.consumers), len(broker.topics[topic_idx].partitions))
	fmt.Println("[Assignment]")
	for _, consumer := range group.consumers {
		parts := make([]string, 0, len(consumer.partitionsOffset))
		for _, p := range consumer.partitionsOffset {
			parts = append(parts, fmt.Sprintf("P%d", p.partitionID))
		}
		fmt.Printf("Consumer %d → %s\n", consumer.ConsumerID, strings.Join(parts, ", "))
	}
	fmt.Println()
	for _, consumer := range group.consumers {
		broker.mu.Lock()
		stream_rw := consumer.stream_rw
		ass := &Message{
			ASSIGNMENT: &Assignment{
				assignment: consumer.partitionsOffset,
			},
		}
		broker.mu.Unlock()
		fmt.Printf("[Broker] Sending ASSIGNMENT\n  Consumer: %d\n  Partitions: %s\n", consumer.ConsumerID, strings.Join(func() []string {
			s := make([]string, 0, len(consumer.partitionsOffset))
			for _, p := range consumer.partitionsOffset {
				s = append(s, fmt.Sprintf("P%d", p.partitionID))
			}
			return s
		}(), ", "))
		err = WriteMessageToStream(stream_rw, ass)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("===============================")

	return err
}

// Khi nhận được message có thuộc tính PRODUCER_REGISTER
// Thì sẽ tiến hành parse port được gửi lên từ client và kết nối đến port đó
// sau đó tạo ra 1 tiến trình riêng để lắng nghe và phản hồi các message được gửi lên từ kết nối này
func (broker *Broker) processProducerRegisterMessage(producer_register_message *ProducerRegister) (*byte, error) {
	fmt.Printf("\n Have a port : %v", producer_register_message.port)
	var topic_idx int = -1
	if len(broker.topics) == 0 {
		ntopic := &Topic{}
		ntopic.init(producer_register_message.topicID)
		broker.topics = append(broker.topics, *ntopic)
		topic_idx = len(broker.topics) - 1
	} else {
		for index, tp := range broker.topics {
			if tp.topicID == producer_register_message.topicID {
				topic_idx = index
				break
			}
		}
		if topic_idx == -1 {
			ntopic := &Topic{}
			ntopic.init(producer_register_message.topicID)
			broker.topics = append(broker.topics, *ntopic)
			topic_idx = len(broker.topics) - 1
		}
	}
	go func() { // dial to the producer and send a message to it to confirm the registration

		// connect to the server on localhost:some port
		conn, _ := net.Dial("tcp",
			fmt.Sprintf(":%d", producer_register_message.port))
		//

		defer conn.Close()
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsed_message, err := ReadMessageFromStream(stream_rw)
			if parsed_message == nil || err != nil {
				panic(err)
			}
			var resp *byte = nil
			if parsed_message.PCM != nil {
				resp, err = broker.processProducerPCM(parsed_message.PCM, topic_idx)
				if err != nil {
					panic(err)
				}
			}
			err = WriteMessageToStream(stream_rw, &Message{R_PCM: resp})
			if err != nil {
				panic(err)
			}
		}
	}()
	var resp byte = 0
	return &resp, nil
}
