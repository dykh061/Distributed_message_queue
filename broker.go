package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

const BROKER_PORT = 10000

type Broker struct {
	topics []Topic
}

func (broker *Broker) init() {
	broker.topics = make([]Topic, 0, 10)
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
	broker.topics[idx].mq.push(pcm_message)
	broker.topics[idx].mq.debug()
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
			if tp.TopicID == consumer_register_message.topicID {
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
		ncgroup := &CGroup{}
		ncgroup.init(consumer_register_message.groupID)
		broker.topics[topic_idx].cgroups = append(broker.topics[topic_idx].cgroups, *ncgroup)
		cgroup_idx = len(broker.topics[topic_idx].cgroups) - 1
	} else {
		for index, cg := range broker.topics[topic_idx].cgroups {
			if cg.cgroupId == consumer_register_message.groupID {
				cgroup_idx = index
				break
			}
		}
		if cgroup_idx == -1 {
			ncgroup := &CGroup{}
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
		broker.topics[topic_idx].cgroups[cgroup_idx].consumers = append(
			broker.topics[topic_idx].cgroups[cgroup_idx].consumers,
			Consumers{status: true, conn: conn},
		)
		for {
			offset := broker.topics[topic_idx].cgroups[cgroup_idx].offset
			data := broker.topics[topic_idx].mq.peek(uint(offset))
			if data == nil {
				continue
			}
			for _, consumer := range broker.topics[topic_idx].cgroups[cgroup_idx].consumers {
				if consumer.status == true {
					stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
					consumer.status = false
					err := WriteMessageToStream(stream_rw, &Message{PCM: data})
					if err != nil {
						panic(err)
					}
					resp, err := ReadMessageFromStream(stream_rw)
					if err != nil {
						panic(err)
					}
					if resp.RESPONSE_CONSUMER_REGISTER != nil {
						consumer.status = true
						broker.topics[topic_idx].cgroups[cgroup_idx].offset += 1
					}
				}
			}
		}
	}()
	var resp byte = 0
	return &resp, nil

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
			if tp.TopicID == producer_register_message.topicID {
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
