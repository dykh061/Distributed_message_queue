package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

type Consumer struct {
	port           uint16
	topicID        uint16
	groupID        uint16
	assignment     []PartitionOffset // danh sách các partition mà consumer này được assign và offset mà consumer này đang đọc tới đâu
	commitedOffset []PartitionOffset // Lưu các offset đã commit của từng partition
	generation     uint16            // phiên bản của assignment hiện tại
	pendingCommit  map[uint16]uint32 // lưu các offset đang chờ commit của từng partition
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
		fmt.Println("[Broker] Consumer registration failed")
	} else {
		fmt.Printf("[Consumer %d] Listening on :%d\n", consumer.port, consumer.port)
		fmt.Printf("[Broker] CONSUMER_REGISTER\n  Consumer: %d\n  Group: %d\n  Port: %d\n", consumer.port, consumer.groupID, consumer.port)
		fmt.Printf("[Broker] Connecting to Consumer :%d\n", consumer.port)
		fmt.Printf("[Broker] Consumer %d connected\n", consumer.port)
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
				fmt.Printf("[Consumer %d] ASSIGNMENT received\n", consumer.port)
				fmt.Printf("[Consumer %d] Subscribed: ", consumer.port)
				for i, p := range consumer.assignment {
					if i > 0 { fmt.Print(", ") }
					fmt.Printf("P%d", p.partitionID)
				}
				fmt.Println()
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
				messageCount := 0
				if resp.FETCH_ACK.found {
					for _, p := range consumer.assignment {
						if p.partitionID == resp.FETCH_ACK.partitionID {
							messageCount = int(resp.FETCH_ACK.nextOffset - p.offset)
							break
						}
					}
				}
				fmt.Printf("[Consumer %d] FETCH_ACK received\n  Partition: P%d\n  MessageCount: %d\n", consumer.port, resp.FETCH_ACK.partitionID, messageCount)
				err := consumer.handleFetchAck(stream_rw, resp.FETCH_ACK)
				if err != nil {
					return err
				}
			}
			if resp.COMMIT_OFFSET_ACK != nil {
				commitAck := resp.COMMIT_OFFSET_ACK
				fmt.Printf("[Consumer %d] COMMIT_OFFSET_ACK received\n  Partition: P%d\n  CommittedOffset: %d\n", consumer.port, commitAck.partitionID, commitAck.offset)
				err := consumer.updateCommitedOffset(commitAck.partitionID, commitAck.offset)
				if err != nil {
					return err
				}
				err = consumer.fetchMessages(stream_rw, commitAck.partitionID, commitAck.offset)
				if err != nil {
					return err
				}
			}
		}

	}
	return nil
}

func (consumer *Consumer) updateCommitedOffset(partitionID uint16, offset uint32) error {
	value, ok := consumer.pendingCommit[partitionID]
	if ok && value == offset {
		delete(consumer.pendingCommit, partitionID)
		for i, po := range consumer.commitedOffset {
			if po.partitionID == partitionID {
				consumer.commitedOffset[i].offset = offset
				return nil
			}
		}
		consumer.commitedOffset = append(consumer.commitedOffset, PartitionOffset{
			partitionID: partitionID,
			offset:      offset,
		})
		return nil
	}
	return errors.New("offset not found in pending commit")
}

func (consumer *Consumer) fetchMessages(steam_rw *bufio.ReadWriter, partitionID uint16, offset uint32) error {
	fmt.Printf("[Consumer %d] FETCH\n  Partition: P%d\n  Offset: %d\n  MaxMessages: 100\n", consumer.port, partitionID, offset)
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
		fmt.Printf("[Consumer %d] No new messages\n", consumer.port)
		fmt.Printf("[Consumer %d] Retry fetch in 1s\n", consumer.port)
		time.Sleep(1 * time.Second)
		return consumer.fetchMessages(stream_rw, resp.partitionID, resp.nextOffset)
	}
	startOffset := resp.nextOffset - uint32(len(resp.data)/2)
	messageCount := resp.nextOffset - startOffset
	fmt.Printf("[Consumer %d] Processing messages\n  Partition: P%d\n  StartOffset: %d\n  MessageCount: %d\n  NextOffset: %d\n", consumer.port, resp.partitionID, startOffset, messageCount, resp.nextOffset)
	commitOffset := consumer.readMsgFromPartitionLog(resp.data, resp.nextOffset, resp.partitionID)
	fmt.Printf("[Consumer %d] Process completed\n  NextOffset: %d\n", consumer.port, commitOffset)
	return consumer.commitOffset(stream_rw, resp.partitionID, commitOffset)
}

func (consumer *Consumer) readMsgFromPartitionLog(msg []byte, nextOffset uint32, partitionID uint16) uint32 {
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
	consumer.pendingCommit[partitionID] = offset
	fmt.Printf("[Consumer %d] COMMIT_OFFSET\n  Group: %d\n  Partition: P%d\n  Offset: %d\n", consumer.port, consumer.groupID, partitionID, offset)
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
