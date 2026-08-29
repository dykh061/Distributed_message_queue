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
	mu     sync.RWMutex
	topics []*Topic
}

func (broker *Broker) init() {
	broker.topics = make([]*Topic, 0)
}

func (broker *Broker) printState(topic *Topic) {
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	fmt.Println("\n================ BROKER STATE ================")
	fmt.Printf("TOPIC: %d\n", topic.topicID)
	fmt.Print("Partition: \n")
	for i := range (*topic).partitions {
		partition := (*topic).partitions[i]
		fmt.Printf("P%d\n  Messages: %d\n\n", (*partition).partitionID, (*partition).mq.getCount())
	}
	if len(topic.cgroups) == 0 {
		fmt.Println("================================================")
		return
	}

	for i := range topic.cgroups {
		muCG := &topic.cgroups[i].mu
		muCG.RLock()
		cg := topic.cgroups[i]
		fmt.Printf("GROUP: %d\n\n", cg.cgroupID)
		fmt.Print("Member: \n\n")
		for i := range cg.consumers {
			fmt.Printf("Consumer %d\n", (*cg).consumers[i].ConsumerID)
			fmt.Printf("Assignment:  -> ")
			result := make([]string, 0, len((*cg).consumers[i].partitionsOffset))
			for _, p := range (*cg).consumers[i].partitionsOffset {
				result = append(result, fmt.Sprintf("P%d", p.partitionID))
			}
			fmt.Printf("%s ", strings.Join(result, ", "))
		}
		fmt.Print("Offset: \n\n")
		for i := range (*cg).offset {
			po := (*cg).offset[i]
			fmt.Printf("  P%d -> Next: %d | Last Committed Msg: %d\n", po.partitionID, po.offset, po.offset-1)
		}

		muCG.RUnlock()
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

func (broker *Broker) processProducerPCM(pcm_message []byte, topic *Topic) (*byte, error) {
	fmt.Printf("[Broker] PCM\n  Producer Push Msg :\n  Topic: %d\n", topic.topicID)
	partition := topic.selectNextPartition()
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
	broker.mu.Lock()
	var topic_idx int = -1
	var topic *Topic = nil
	if len(broker.topics) == 0 {
		ntopic := &Topic{}
		ntopic.init(consumer_register_message.topicID)
		broker.topics = append(broker.topics, ntopic)
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
			broker.topics = append(broker.topics, ntopic)
			topic_idx = len(broker.topics) - 1
		}
	}
	topic = broker.topics[topic_idx]
	broker.mu.Unlock()
	topicMutex := &topic.mu
	topicMutex.Lock()
	var cgroup_idx int = -1
	var consumerGroup *ConsumerGroup = nil
	if len(topic.cgroups) == 0 {
		ncgroup := &ConsumerGroup{}
		ncgroup.init(consumer_register_message.groupID)
		(*topic).cgroups = append((*topic).cgroups, ncgroup)
		cgroup_idx = len((*topic).cgroups) - 1
	} else {
		for index := range (*topic).cgroups {
			cg := (*topic).cgroups[index]
			if (*cg).cgroupID == consumer_register_message.groupID {
				cgroup_idx = index
				break
			}
		}
		if cgroup_idx == -1 {
			ncgroup := &ConsumerGroup{}
			ncgroup.init(consumer_register_message.groupID)
			(*topic).cgroups = append((*topic).cgroups, ncgroup)
			cgroup_idx = len((*topic).cgroups) - 1
		}
	}
	consumerGroup = (*topic).cgroups[cgroup_idx]
	topicMutex.Unlock()
	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", consumer_register_message.port))
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		cgMu := &consumerGroup.mu
		cgMu.Lock()
		consumerID := (*consumerGroup).nextConsumerID + 1
		(*consumerGroup).consumers = append(
			(*consumerGroup).consumers,
			&Consumers{
				conn:       conn,
				stream_rw:  stream_rw,
				ConsumerID: consumerID,
			},
		)
		(*consumerGroup).nextConsumerID++
		fmt.Printf("[Broker] Connecting to Consumer :%d\n", consumer_register_message.port)
		fmt.Printf("[Broker] Consumer connected\n")
		cgMu.Unlock()
		(*consumerGroup).rebalanceConsumerGroup(topic.partitions)
		broker.sendAssignmentToConsumerGroup(topic, consumerGroup)
		broker.printState(topic)
		err = broker.handleConsumerConnection(stream_rw, consumerID, topic, consumerGroup)
		if err != nil {
			panic(err)
		}
	}()
	var resp byte = 0
	return &resp, nil

}

func (broker *Broker) handleConsumerConnection(stream_rw *bufio.ReadWriter, consumerID uint16, topic *Topic, cgroup *ConsumerGroup) error {
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
				if CommitOffset.GroupID != cgroup.cgroupID ||
					CommitOffset.TopicID != topic.topicID {
					return errors.New("Commit offset message has mismatched group or topic ID")
				}
				success := cgroup.commitOffset(partitionID, CommitOffset.Offset)
				fmt.Println("[Broker] Updating committed offset")
				fmt.Printf("  group-%d\n    P%d: %d → %d\n", CommitOffset.GroupID, partitionID, cgroup.getOffset(partitionID), CommitOffset.Offset)
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
				broker.printState(topic)
			}
			if resp.FETCH != nil {
				cgroup.mu.RLock()
				partitionID := resp.FETCH.partitionID
				listconsumer := cgroup.consumers
				cidx := -1
				for i := range listconsumer {
					if listconsumer[i].ConsumerID == consumerID {
						cidx = i
						break
					}
				}
				if cidx == -1 { // còn optimize chỗ này
					fmt.Printf("[Broker] Consumer %d not found in consumer group %d\n", consumerID, cgroup.cgroupID)
					cgroup.mu.RUnlock()
					continue
				}
				consumer := listconsumer[cidx]
				parts := consumer.partitionsOffset
				flag := false
				for i := range parts {
					if parts[i].partitionID == partitionID {
						flag = true
						break
					}
				}

				if !flag {
					fmt.Printf("[Broker] Consumer %d is not assigned to partition P%d\n", consumerID, partitionID)
					cgroup.mu.RUnlock()
					continue
				}
				if resp.FETCH.generation != cgroup.generation {
					fmt.Printf("[Broker] Consumer %d has outdated generation %d, current generation is %d\n", consumerID, resp.FETCH.generation, cgroup.generation)
					cgroup.mu.RUnlock()
					continue
				}
				cgroup.mu.RUnlock()

				fmt.Printf("[Broker] FETCH received\n  Consumer: %d\n  Group: %d\n  Partition: P%d\n  Offset: %d\n", consumer.ConsumerID, cgroup.cgroupID, partitionID, resp.FETCH.offset)
				var partitionIDX int = -1
				topic.mu.RLock()
				for i := range topic.partitions {
					if topic.partitions[i].partitionID == partitionID {
						partitionIDX = i
						break
					}
				}
				if partitionIDX == -1 {
					topic.mu.RUnlock()
					fmt.Printf("[Broker] Partition P%d not found in topic %d\n", partitionID, topic.topicID)
					continue
				}

				messages, nextOffset, found := topic.partitions[partitionIDX].mq.fetchMessage(uint16(100), resp.FETCH.offset)
				if !found {
					fmt.Printf("[Broker] No messages available\n")
				} else {
					messageCount := int(nextOffset - resp.FETCH.offset)
					fmt.Printf("[Broker] Reading P%d\n  Available messages: %d\n", partitionID, messageCount)
					fmt.Printf("[Broker] Sending FETCH_ACK\n  Consumer: %d\n  Partition: P%d\n  StartOffset: %d\n  MessageCount: %d\n  NextOffset: %d\n", consumer.ConsumerID, partitionID, resp.FETCH.offset, messageCount, nextOffset)
				}
				fetch_ack := &Message{
					FETCH_ACK: &FetchAck{
						partitionID: topic.partitions[partitionIDX].partitionID,
						found:       found,
						nextOffset:  nextOffset,
						data:        messages,
						generation:  resp.FETCH.generation,
					},
				}
				topic.mu.RUnlock()
				err = WriteMessageToStream(stream_rw, fetch_ack)
				if err != nil {
					fmt.Printf("Error writing fetch ack to stream: %v\n", err)
				}

			}
		}
	}
}

func (broker *Broker) sendAssignmentToConsumerGroup(topic *Topic, group *ConsumerGroup) error {
	var err error
	fmt.Println("========== REBALANCE ==========")
	fmt.Printf("Group: %d\nConsumers: %d\nPartitions: %d\n\n", group.cgroupID, len(group.consumers), len(topic.partitions))
	fmt.Println("[Assignment]")
	group.mu.RLock()
	type Task struct {
		stream  *bufio.ReadWriter
		message *Message
	}
	var task []Task
	for _, consumer := range group.consumers {
		parts := make([]string, 0, len(consumer.partitionsOffset))
		for _, p := range consumer.partitionsOffset {
			parts = append(parts, fmt.Sprintf("P%d", p.partitionID))
		}
		fmt.Printf("Consumer %d → %s\n", consumer.ConsumerID, strings.Join(parts, ", "))
	}

	fmt.Println()
	for _, consumer := range group.consumers {
		task = append(task, Task{
			stream: consumer.stream_rw,
			message: &Message{
				ASSIGNMENT: &Assignment{
					assignment: consumer.partitionsOffset,
					generation: group.generation,
				},
			},
		})
		fmt.Printf("[Broker] Sending ASSIGNMENT\n  Consumer: %d\n  Partitions: %s\n", consumer.ConsumerID, strings.Join(func() []string {
			s := make([]string, 0, len(consumer.partitionsOffset))
			for _, p := range consumer.partitionsOffset {
				s = append(s, fmt.Sprintf("P%d", p.partitionID))
			}
			return s
		}(), ", "))
	}
	group.mu.RUnlock()
	for _, t := range task {
		err = WriteMessageToStream(t.stream, t.message)
		if err != nil {
			return err
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
	broker.mu.Lock()
	var topic_idx int = -1
	if len(broker.topics) == 0 {
		ntopic := &Topic{}
		ntopic.init(producer_register_message.topicID)
		broker.topics = append(broker.topics, ntopic)
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
			broker.topics = append(broker.topics, ntopic)
			topic_idx = len(broker.topics) - 1
		}
	}
	topic := broker.topics[topic_idx]
	broker.mu.Unlock()
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
				resp, err = broker.processProducerPCM(parsed_message.PCM, topic)
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
