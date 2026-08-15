package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
)

type Consumer struct {
	port       uint16
	topicID    uint16
	groupID    uint16
	assignment []PartitionOffset // danh sách các partition mà consumer này được assign
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
				err := consumer.handleFetchAck(stream_rw, resp.FETCH_ACK)
				if err != nil {
					return err
				}
				msg, err := ReadMessageFromStream(stream_rw)
				if err != nil {
					return err
				}
				if msg.COMMIT_OFFSET_ACK != nil {
					fmt.Printf("Consumer received commit offset ack: %v\n", *msg.COMMIT_OFFSET_ACK)

				} else {

				}
			}
		}

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

func (consumer *Consumer) handleFetchAck(stream_rw *bufio.ReadWriter, resp *FetchAck) error {
	if !resp.found {
		return errors.New("don't have messages")
	}
	commitOffset := consumer.readMsgFromPartitionLog(resp.data, resp.nextOffset, resp.partitionID)
	return consumer.commitOffset(stream_rw, resp.partitionID, commitOffset)
}

func (consumer *Consumer) 	readMsgFromPartitionLog(msg []byte, nextOffset uint32, partitionID uint16) uint32 {
	var currentOffset uint32
	var partitionIDX int = -1
	var commitOffset uint32
	for i, p := range consumer.assignment {
		if p.partitionID == partitionID {
			currentOffset = p.offset
			partitionIDX = i
			break
		}
	}
	pos := 0
	msgCount := nextOffset - currentOffset
	for i := uint32(0); i < msgCount; i++ {
		lenght := uint16(msg[pos])<<8 + uint16(msg[pos+1])
		pos += 2                              // bỏ qua 2 byte độ dài của message
		message := msg[pos : pos+int(lenght)] // lấy nội dùng message ra
		fmt.Printf("Consumer received message from partition %d, offset %d: %s\n", partitionID, currentOffset+i, string(message))
		commitOffset = currentOffset + i + 1 // cập nhật offset mới nhất đã đọc được + 1
		consumer.assignment[partitionIDX].offset = commitOffset
		pos += int(lenght)
	}
	return commitOffset
}

func (consumer *Consumer) commitOffset(stream_rw *bufio.ReadWriter, partitionID uint16, offset uint32) error {
	Message := &Message{
		COMMIT_OFFSET: &CommitOffset{
			TopicID:     consumer.topicID,
			GroupID:     consumer.groupID,
			PartitionID: partitionID,
			Offset:      offset,
		},
	}
	return WriteMessageCommitOffsetToStream(stream_rw, COMMIT_OFFSET, Message.COMMIT_OFFSET)
}
