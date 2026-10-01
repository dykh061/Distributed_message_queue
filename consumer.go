package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

var ErrConsumerNoLongerInGroup = errors.New("consumer is no longer in consumer group")

type Consumer struct {
	port           uint16
	topicID        uint16
	groupID        uint16
	assignment     []PartitionOffset // danh sách các partition mà consumer này được assign và offset mà consumer này đang đọc tới đâu
	commitedOffset []PartitionOffset // Lưu các offset đã commit của từng partition
	generation     uint32            // phiên bản của assignment hiện tại
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
		printEvent("CONSUMER REGISTRATION FAILED", fmt.Sprintf("Consumer: %d", consumer.port))
	} else {
		printEvent(fmt.Sprintf("CONSUMER %d JOINED", consumer.port), fmt.Sprintf("Listening on :%d", consumer.port), fmt.Sprintf("Group: %d", consumer.groupID), fmt.Sprintf("Topic: %d", consumer.topicID), "Broker connection established")
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
	defer l.Close()

	for {
		// connect to broker to send register
		err = consumer.registerWithBroker()
		if err != nil {
			return err
		}

		conn, err := l.Accept()
		if err != nil {
			return err
		}

		err = consumer.handleBrokerConnection(conn)
		conn.Close()

		if err == ErrConsumerNoLongerInGroup {
			printEvent("CONSUMER REJOINING", fmt.Sprintf("Consumer: %d", consumer.port), "Previous assignment expired", "Requesting a new group assignment")

			continue
		}
		return err
	}

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

func (consumer *Consumer) fetchMessages(stream_rw *bufio.ReadWriter, partitionID uint16, offset uint32) error {
	Message := &Message{
		FETCH: &Fetch{
			partitionID: partitionID,
			offset:      offset,
			generation:  consumer.generation,
		},
	}
	return WriteMessageToStream(stream_rw, Message)
}

func (consumer *Consumer) handleFetchAck(stream_rw *bufio.ReadWriter, resp *FetchAck) error {

	if !resp.found { // Luôn retry sau 1 giây
		time.Sleep(1 * time.Second)
		return consumer.fetchMessages(stream_rw, resp.partitionID, resp.nextOffset) // gửi lại fetch
	}
	commitOffset := consumer.readMsgFromPartitionLog(resp.data, resp.nextOffset, resp.partitionID)
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
		printEvent(fmt.Sprintf("NEW MESSAGE %d", i), fmt.Sprintf("Partition: P%d", partitionID), fmt.Sprintf("Offset: %d", currentOffset+i), fmt.Sprintf("Payload: %s", string(message)))
		commitOffset = currentOffset + i + 1 // cập nhật offset mới nhất đã đọc được + 1
		consumer.assignment[partitionIDX].offset = commitOffset
		pos += int(lenght)
	}
	return commitOffset
}

func (consumer *Consumer) commitOffset(stream_rw *bufio.ReadWriter, partitionID uint16, offset uint32) error {
	consumer.pendingCommit[partitionID] = offset
	printEvent(fmt.Sprintf("CONSUMER %d COMMIT", consumer.port), fmt.Sprintf("Group: %d", consumer.groupID), fmt.Sprintf("Partition: P%d", partitionID), fmt.Sprintf("Offset: %d", offset), fmt.Sprintf("Generation: %d", consumer.generation))
	Message := &Message{
		COMMIT_OFFSET: &CommitOffset{
			TopicID:     consumer.topicID,
			GroupID:     consumer.groupID,
			PartitionID: partitionID,
			Offset:      offset,
			Generation:  consumer.generation,
		},
	}
	return WriteMessageCommitOffsetToStream(stream_rw, COMMIT_OFFSET, Message.COMMIT_OFFSET)
}

func (consumer *Consumer) handleBrokerConnection(conn net.Conn) error {
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	for {
		resp, err := ReadMessageFromStream(stream_rw)
		if err != nil {
			log.Println(err)
			return err
		}
		if resp == nil {
			continue
		} else {
			if resp.ERROR_CONSUMER_NO_LONGER_IN_GROUP != nil {
				printEvent("CONSUMER REMOVED FROM GROUP", fmt.Sprintf("Consumer: %d", consumer.port), "Waiting to rejoin")
				return ErrConsumerNoLongerInGroup
			}
			if resp.ASSIGNMENT != nil {
				consumer.assignment = resp.ASSIGNMENT.assignment
				consumer.generation = resp.ASSIGNMENT.generation
				consumer.pendingCommit = make(map[uint16]uint32)
				if len(consumer.assignment) == 0 {
					var ack = byte(1)
					err := WriteMessageToStream(stream_rw, &Message{ASSIGNMENT_ACK: &ack})
					if err != nil {
						return err
					}
					continue
				}
				var ack = byte(1)
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
				if resp.FETCH_ACK.errCode != 0 {
					switch resp.FETCH_ACK.errCode {
					case ERR_NOT_ASSIGNED:

						printEvent("FETCH REJECTED", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("Partition P%d is not assigned", resp.FETCH_ACK.partitionID))
						continue

					case ERR_ILLEGAL_GENERATION:

						printEvent("FETCH REJECTED", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("Illegal generation for P%d", resp.FETCH_ACK.partitionID))
						continue

					case ERR_PARTITION_NOT_FOUND:

						printEvent("FETCH REJECTED", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("Partition P%d not found", resp.FETCH_ACK.partitionID))
						continue
					default:
						printEvent("FETCH REJECTED", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("Error %d for P%d", resp.FETCH_ACK.errCode, resp.FETCH_ACK.partitionID))
						continue
					}
				}
				if resp.FETCH_ACK.generation != consumer.generation {
					continue // phiên bản assingment của log message không khớp với phiên bản hiện tại của consumer, bỏ qua message này
				}
				err := consumer.handleFetchAck(stream_rw, resp.FETCH_ACK)
				if err != nil {
					return err
				}
			}
			if resp.COMMIT_OFFSET_ACK != nil {
				commitAck := resp.COMMIT_OFFSET_ACK
				if !commitAck.success {
					printEvent("COMMIT REJECTED", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("Partition: P%d", commitAck.partitionID))
					continue
				}
				if commitAck.generation != consumer.generation {
					printEvent("STALE COMMIT ACK", fmt.Sprintf("Consumer: %d", consumer.port), fmt.Sprintf("ACK generation: %d", commitAck.generation), fmt.Sprintf("Current generation: %d", consumer.generation))
					continue
				}
				printEvent(fmt.Sprintf("CONSUMER %d COMMIT ACK", consumer.port), fmt.Sprintf("Partition: P%d", commitAck.partitionID), fmt.Sprintf("Committed offset: %d", commitAck.offset))
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
}
