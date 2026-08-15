package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
)

const BROKER_PORT = 10000

type Broker struct {
	topics []Topic
}

func (broker *Broker) init() {
	broker.topics = make([]Topic, 0)
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
	partition := broker.topics[idx].selectNextPartition()
	if partition == nil {
		err := errors.New("No partition available for topic")
		return nil, err
	}
	partition.mq.push(pcm_message)
	partition.mq.debug()
	var ack byte = 0
	return &ack, nil
}

func (broker *Broker) processEchoMessage(echo_message *string) (string, error) {
	return fmt.Sprintf("I have receive : %s", *echo_message), nil
}

func (broker *Broker) processConsumerGroupConsump(consumer_register_message *ConsumerRegister) (*byte, error) {
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
		broker.topics[topic_idx].cgroups[cgroup_idx].consumers = append(
			broker.topics[topic_idx].cgroups[cgroup_idx].consumers,
			Consumers{
				conn:       conn,
				stream_rw:  stream_rw,
				ConsumerID: uint16(len(broker.topics[topic_idx].cgroups[cgroup_idx].consumers)),
			},
		)

		broker.topics[topic_idx].rebalanceConsumerGroup(cgroup_idx)
		broker.sendAssignmentToConsumerGroup(topic_idx, cgroup_idx)
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
				fmt.Printf("Consumer acknowledged assignment")
			}
			if resp.COMMIT_OFFSET != nil {
				CommitOffset := resp.COMMIT_OFFSET
				partitionID := CommitOffset.PartitionID
				if CommitOffset.GroupID != broker.topics[topic_idx].cgroups[cgroup_idx].cgroupID ||
					CommitOffset.TopicID != broker.topics[topic_idx].topicID {
					return errors.New("Commit offset message has mismatched group or topic ID")
				}
				sliceOffset := broker.topics[topic_idx].cgroups[cgroup_idx].offset
				flag := false
				for _, po := range sliceOffset {
					if po.partitionID == partitionID {
						po.offset = CommitOffset.Offset
						flag = true
						break
					}
				}
				if !flag {
					sliceOffset = append(sliceOffset, PartitionOffset{
						partitionID: partitionID,
						offset:      CommitOffset.Offset,
					})
				}
				var ack byte = 1
				Message := &Message{
					COMMIT_OFFSET_ACK: &ack,
				}
				err := WriteMessageToStream(stream_rw, Message)
				if err != nil {
					return err
				}
			}
			if resp.FETCH != nil {
				partitionID := resp.FETCH.partitionID
				var partition *Partition = nil
				for i := range broker.topics[topic_idx].partitions {
					if broker.topics[topic_idx].partitions[i].partitionID == partitionID {
						partition = &broker.topics[topic_idx].partitions[i]
						break
					}
				}
				if partition != nil {
					messages, nextOffset, found := broker.fetchMessagesFromPartition(&partition.mq, uint16(100), resp.FETCH.offset)
					if !found {
						fmt.Printf("Error fetching messages from partition %d: %v\n", partitionID, err)
					}
					fetch_ack := &Message{
						FETCH_ACK: &FetchAck{
							partitionID: partition.partitionID,
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

func (broker *Broker) fetchMessagesFromPartition(partition *Queue, maxMessages uint16, offset uint32) (data []byte, nextOffset uint32, found bool) {

	if partition.count == 0 {
		return nil, offset, false
	}

	// Ví dụ base = 100, count = 3 → log chứa 100, 101, 102.
	// maxLogicOffset = 103.
	maxLogicOffset := partition.baseOffset + uint64(partition.count)

	// offset nhỏ hơn baseOffset: record đã bị retention xoá.
	// offset >= maxLogicOffset: chưa có record mới.
	if uint64(offset) < partition.baseOffset || uint64(offset) >= maxLogicOffset {
		return nil, offset, false
	}

	remaining := maxLogicOffset - uint64(offset)

	readCount := uint64(maxMessages)
	if readCount > remaining {
		readCount = remaining
	}
	nextOffset = offset
	var messages []byte
	for i := uint64(0); i < readCount; i++ {
		relativeOffset := nextOffset - uint32(partition.baseOffset)
		msg := partition.peek(uint(relativeOffset))
		lengthmsg := uint16(len(msg))
		first := byte(lengthmsg >> 8)
		last := byte(lengthmsg & 0xff)
		messages = append(messages, first, last)
		messages = append(messages, msg...)
		nextOffset++
	}
	return messages, nextOffset, true
}

func (broker *Broker) sendAssignmentToConsumerGroup(topic_idx, cgroup_idx int) error {
	var err error

	group := &broker.topics[topic_idx].cgroups[cgroup_idx]

	for _, consumer := range group.consumers {
		stream_rw := consumer.stream_rw
		ass := &Message{
			ASSIGNMENT: &Assignment{
				assignment: consumer.partitionsOffset,
			},
		}
		err = WriteMessageToStream(stream_rw, ass)
		if err != nil {
			panic(err)
		}
	}

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
