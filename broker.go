package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
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

// Khởi động server broker và lắng nghe các kết nối đến port
// khi có kết nối đến thì đồng ý kết nối và đọc dữ liệu từ stream
// sau đó parse dữ liệu và xữ lý dữ liệu cuối cùng gửi lại phản hồi về
func (broker *Broker) StartBrokerServer() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return err
	}
	defer l.Close()
	printEvent("BROKER ONLINE", fmt.Sprintf("Listening on :%d", BROKER_PORT), "Topics initialized", "Waiting for producers and consumers")
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
	partition := topic.selectNextPartition()
	if partition == nil {
		err := errors.New("No partition available for topic")
		return nil, err
	}
	partition.mq.push(pcm_message)
	printEvent("MESSAGE ROUTED", fmt.Sprintf("Topic: %d", topic.topicID), fmt.Sprintf("Partition: P%d", partition.partitionID))
	var ack byte = 0
	return &ack, nil
}

func (broker *Broker) processEchoMessage(echo_message *string) (string, error) {
	return fmt.Sprintf("I have receive : %s", *echo_message), nil
}

func (broker *Broker) processConsumerGroupConsump(consumer_register_message *ConsumerRegister) (*byte, error) {
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
			printEvent("CONSUMER CONNECTION ERROR", fmt.Sprintf("Consumer: %d", consumer_register_message.port), fmt.Sprintf("Error: %v", err))
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
		printEvent("CONSUMER CONNECTED", fmt.Sprintf("Consumer: %d", consumer_register_message.port), "Broker connection established")
		cgMu.Unlock()
		(*consumerGroup).rebalanceConsumerGroup(topic.partitions)
		broker.sendAssignmentToConsumerGroup(topic, consumerGroup)
		err = broker.handleConsumerConnection(stream_rw, consumerID, topic, consumerGroup)
		if err != nil {
			fmt.Printf(
				"[Broker] Consumer %d connection closed: %v\n",
				consumerID,
				err,
			)
			cleanupErr := broker.removeConsumerFromGroup(consumerID, consumerGroup, topic)
			if cleanupErr != nil {
				fmt.Printf("[Broker] Cleanup failed: %v\n", cleanupErr)
			}
			return
		}
	}()
	var resp byte = 0
	return &resp, nil

}

func (broker *Broker) handleConsumerConnection(stream_rw *bufio.ReadWriter, consumerID uint16, topic *Topic, cgroup *ConsumerGroup) error {
	for {
		resp, err := ReadMessageFromStream(stream_rw)
		if err != nil {
			return err

		}
		if resp != nil {
			if resp.ASSIGNMENT_ACK != nil {
			}
			if resp.COMMIT_OFFSET != nil {
				CommitOffset := resp.COMMIT_OFFSET
				partitionID := CommitOffset.PartitionID
				cgroup.mu.RLock()
				generation := cgroup.generation
				cgroup.mu.RUnlock()
				if CommitOffset.GroupID != cgroup.cgroupID ||
					CommitOffset.TopicID != topic.topicID {
					fmt.Printf("[Broker] Invalid COMMIT_OFFSET rejected: mismatched Group or Topic (got Group %d, Topic %d)\n",
						CommitOffset.GroupID, CommitOffset.TopicID)
					var ack = CommitOffsetAck{
						partitionID: partitionID,
						offset:      CommitOffset.Offset,
						generation:  CommitOffset.Generation,
						success:     false,
					}
					Message := &Message{
						COMMIT_OFFSET_ACK: &ack,
					}
					err := WriteSerializableToStream(stream_rw, COMMIT_OFFSET_ACK, Message.COMMIT_OFFSET_ACK)
					if err != nil {
						return err
					}
					continue
				}

				if CommitOffset.Generation != generation {
					fmt.Printf("[Broker] Outdated COMMIT_OFFSET rejected\n  Consumer: %d\n  Partition: P%d\n  CommitGen: %d != CurrentGen: %d\n",
						consumerID, partitionID, CommitOffset.Generation, generation)
					var ack = CommitOffsetAck{
						partitionID: partitionID,
						offset:      CommitOffset.Offset,
						generation:  CommitOffset.Generation,
						success:     false,
					}
					Message := &Message{
						COMMIT_OFFSET_ACK: &ack,
					}
					err := WriteSerializableToStream(stream_rw, COMMIT_OFFSET_ACK, Message.COMMIT_OFFSET_ACK)
					if err != nil {
						return err
					}
					continue
				}
				success := cgroup.commitOffset(partitionID, CommitOffset.Offset)
				var ack = CommitOffsetAck{
					partitionID: partitionID,
					offset:      CommitOffset.Offset,
					generation:  CommitOffset.Generation,
					success:     success,
				}
				Message := &Message{
					COMMIT_OFFSET_ACK: &ack,
				}
				err := WriteSerializableToStream(stream_rw, COMMIT_OFFSET_ACK, Message.COMMIT_OFFSET_ACK)
				if err != nil {
					return err
				}
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
				if cidx == -1 {
					fmt.Printf(
						"[Broker] Consumer %d is no longer a member of consumer group %d\n",
						consumerID,
						cgroup.cgroupID,
					)
					cgroup.mu.RUnlock()
					var st byte = 0
					err := WriteMessageToStream(stream_rw, &Message{ERROR_CONSUMER_NO_LONGER_IN_GROUP: &st})
					if err != nil {
						return err
					}
					return nil
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
					printEvent("FETCH REJECTED", fmt.Sprintf("Consumer: %d", consumerID), fmt.Sprintf("Partition P%d is not assigned", partitionID))
					cgroup.mu.RUnlock()
					err := broker.fetchAckErr(partitionID, false, ERR_NOT_ASSIGNED, resp.FETCH.offset, nil, resp.FETCH.generation, stream_rw)
					if err != nil {

						return err
					}
					continue
				}
				if resp.FETCH.generation != cgroup.generation {
					printEvent("STALE FETCH", fmt.Sprintf("Consumer: %d", consumerID), fmt.Sprintf("Request generation: %d", resp.FETCH.generation), fmt.Sprintf("Current generation: %d", cgroup.generation))
					cgroup.mu.RUnlock()
					err := broker.fetchAckErr(partitionID, false, ERR_ILLEGAL_GENERATION, resp.FETCH.offset, nil, resp.FETCH.generation, stream_rw)
					if err != nil {

						return err
					}
					continue
				}
				cgroup.mu.RUnlock()

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
					printEvent("FETCH ERROR", fmt.Sprintf("Partition P%d not found in topic %d", partitionID, topic.topicID))
					err := broker.fetchAckErr(partitionID, false, ERR_PARTITION_NOT_FOUND, resp.FETCH.offset, nil, resp.FETCH.generation, stream_rw)
					if err != nil {
						return err
					}
					continue
				}

				messages, nextOffset, found := topic.partitions[partitionIDX].mq.fetchMessage(uint16(100), resp.FETCH.offset)
				fetch_ack := &Message{
					FETCH_ACK: &FetchAck{
						partitionID: topic.partitions[partitionIDX].partitionID,
						found:       found,
						errCode:     uint8(ERR_OK),
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
	group.mu.RLock()
	type Task struct {
		stream  *bufio.ReadWriter
		message *Message
	}
	var task []Task
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
	}
	group.mu.RUnlock()
	for _, t := range task {
		err = WriteMessageToStream(t.stream, t.message)
		if err != nil {
			return err
		}
	}
	return err
}

// Khi nhận được message có thuộc tính PRODUCER_REGISTER
// Thì sẽ tiến hành parse port được gửi lên từ client và kết nối đến port đó
// sau đó tạo ra 1 tiến trình riêng để lắng nghe và phản hồi các message được gửi lên từ kết nối này
func (broker *Broker) processProducerRegisterMessage(producer_register_message *ProducerRegister) (*byte, error) {

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
		conn, err := net.Dial("tcp",
			fmt.Sprintf(":%d", producer_register_message.port))
		if err != nil {
			printEvent("PRODUCER CONNECTION ERROR", fmt.Sprintf("Producer: %d", producer_register_message.port), fmt.Sprintf("Error: %v", err))
			return
		}
		printEvent("PRODUCER CONNECTED", fmt.Sprintf("Producer: %d", producer_register_message.port), "Broker connection established")

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

func (broker *Broker) fetchAckErr(partitionID uint16, found bool, errOK uint8, offset uint32, data []byte, generation uint32, stream_rw *bufio.ReadWriter) error {
	var err error = nil
	fetch_ack := &Message{
		FETCH_ACK: &FetchAck{
			partitionID: partitionID,
			found:       found,
			errCode:     errOK,
			nextOffset:  offset,
			data:        data,
			generation:  generation,
		},
	}
	err = WriteMessageToStream(stream_rw, fetch_ack)
	if err != nil {
		return err
	}
	return err
}

func (broker *Broker) removeConsumerFromGroup(consumerID uint16, cgroup *ConsumerGroup, topic *Topic) error {
	var connection net.Conn
	removed := false
	cgroup.mu.Lock()
	for i, consumer := range cgroup.consumers {
		if consumer.ConsumerID != consumerID { // Tìm consuemr có ID trùng với consumerID nếu không trùng thì bỏ qua
			continue
		}
		connection = consumer.conn
		cgroup.consumers = append(cgroup.consumers[:i], cgroup.consumers[i+1:]...) // xóa consumer bằng cách nối 2 mảng trước và sau consumerID lại bỏ consumerID ra
		removed = true
		break
	}
	remainingConsumers := len(cgroup.consumers)
	cgroup.mu.Unlock()
	if !removed {
		return nil
	}
	printEvent("CONSUMER LEFT", fmt.Sprintf("Consumer: %d", consumerID), fmt.Sprintf("Group: %d", cgroup.cgroupID), fmt.Sprintf("Remaining consumers: %d", remainingConsumers))
	connection.Close()
	if remainingConsumers == 0 {
		return nil
	}
	if err := cgroup.rebalanceConsumerGroup(topic.partitions); err != nil {
		return nil
	}
	return broker.sendAssignmentToConsumerGroup(topic, cgroup)
}
