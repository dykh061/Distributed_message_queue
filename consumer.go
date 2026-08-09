package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

type Consumer struct {
	port       uint16
	topicID    uint16
	groupID    uint16
	assignment []partitionOffset // danh sách các partition mà consumer này được assign
	generation uint16            // phiên bản của assignment hiện tại
}

// Kết nối tới Broker.
// Gửi message CONSUMER_REGISTER chứa port mà Consumer đang lắng nghe.
// Chờ Broker phản hồi ACK đăng ký.
// Sau khi hàm kết thúc thì kết nối này sẽ được đóng.
func (consumer *Consumer) registerWithBroker() error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return fmt.Errorf("cannot connect to broker on port %d: %w", BROKER_PORT, err)
	}
	//

	defer conn.Close()
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	message := Message{
		CONSUMER_REGISTER: &ConsumerRegister{
			port:    consumer.port,
			topicID: consumer.topicID,
			groupID: consumer.groupID,
		},
	}

	err = WriteSerializableToStream(stream_rw, CONSUMER_REGISTER, message.CONSUMER_REGISTER)
	if err != nil {
		return err
	}

	resp, err := ReadMessageFromStream(stream_rw)
	if err != nil {
		return err
	}
	if resp.RESPONSE_CONSUMER_REGISTER == nil {
		fmt.Println("Broker did not send register ack")
	} else {
		fmt.Printf("Receive response from broker: %v\n", *resp.RESPONSE_CONSUMER_REGISTER)
	}
	return nil
}

// Khởi động TCP Server của Consumer.
// Đăng ký port của Consumer với Broker.
// Sau khi Broker kết nối ngược lại vào port này,
// Consumer sẽ gửi và nhận message thông qua kết nối đó.
func (consumer *Consumer) StartConsumerServer() error {
	var err error

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", consumer.port))
	if err != nil {
		return err
	}
	// connect to broker to send register
	err = consumer.registerWithBroker()
	if err != nil {
		return err
	}

	defer l.Close()
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
	for {
		resp, err := ReadMessageFromStream(stream_rw)
		if err != nil {
			log.Println(err)
			return err
		}
		if resp == nil {
			continue
		}
		if resp != nil {
			if resp.ASSIGNMENT != nil {
				consumer.assignment = resp.ASSIGNMENT.assignment
				consumer.generation += 1
				var ack = byte(1)
				fmt.Printf("Consumer updated assignment to version %d:", consumer.generation)
				err := WriteMessageToStream(stream_rw, &Message{ASSIGNMENT_ACK: &ack})
				if err != nil {
					return err
				}
				for _, p := range consumer.assignment {
					err := consumer.fetchMessages(stream_rw, p.partitionID, p.offset)
					if err != nil {
						return err
					}
				}
			}
			if resp.FETCH_ACK != nil {
				//xỮ LÍ BATH commit và fetch tiếp theo
			}
		}
		// cái chỗ này để write gửi cho broker yêu cầu fetch à
	}
	return nil
}

func (consumer *Consumer) fetchMessages(steam_rw *bufio.ReadWriter, partitionID uint16, offset uint32) error {
	Message := &Message{
		FETCH: &Fetch{
			partitionID: partitionID,
			offset:      offset,
		},
	}
	return WriteMessageToStream(steam_rw, Message)
}

func (consumer *Consumer) requestFetch(stream_rw *bufio.ReadWriter, resp *FetchAck) error {
	if !resp.found {
		return nil
	}
	// chưa xử lý commit offset về cho broker
}
