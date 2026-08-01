package main

// Message util (
// + Message
// + Read and Write
// + Parse )

import (
	"bufio"
	"fmt"
)

const (
	ECHO              = 1
	PRODUCER_REGISTER = 2
	PCM               = 3 // Producer Consumer Message
	CONSUMER_REGISTER = 4
	COMMIT_OFFSET     = 5
	ASSIGNMENT        = 6
	FETCH             = 7
	// other message types can be added here

	// ACK
	RESPONSE_ECHO              = 101
	RESPONSE_PRODUCER_REGISTER = 102
	R_PCM                      = 103
	RESPONSE_CONSUMER_REGISTER = 104
	ASSIGNMENT_ACK             = 105
	FETCH_ACK                  = 106
)

type Message struct {
	ECHO              *string
	PRODUCER_REGISTER *ProducerRegister
	PCM               []byte
	CONSUMER_REGISTER *ConsumerRegister
	COMMIT_OFFSET     *CommitOffset
	ASSIGNMENT        *Assignment
	FETCH             *Fetch
	// other message types can be added here

	// ACK
	RESPONSE_ECHO              *string
	RESPONSE_PRODUCER_REGISTER *byte
	R_PCM                      *byte
	RESPONSE_CONSUMER_REGISTER *byte
	ASSIGNMENT_ACK             *byte
	FETCH_ACK                  *FetchAck
}
type FetchAck struct {
	partitionID uint16
	found       bool
	offset      uint32
	data        []byte
}

type Fetch struct {
	partitionID uint16
}

type Assignment struct {
	Partitions []uint16
}
type CommitOffset struct {
	TopicID uint16
	GroupID uint16
	Offset  uint32
}

type ProducerRegister struct {
	port    uint16
	topicID uint16
}

func (fa *FetchAck) toByte() []byte {
	data := make([]byte, 7)
	data[0] = byte(fa.partitionID >> 8)
	data[1] = byte(fa.partitionID & 0xFF)
	if fa.found {
		data[2] = 1
	} else {
		data[2] = 0
	}
	data[3] = byte(fa.offset >> 24)
	data[4] = byte((fa.offset >> 16) & 0xFF)
	data[5] = byte((fa.offset >> 8) & 0xFF)
	data[6] = byte(fa.offset & 0xFF)
	data = append(data, fa.data...)
	return data
}

func (fa *FetchAck) fromByte(data []byte) {
	fa.partitionID = uint16(data[0])<<8 + uint16(data[1])
	fa.found = data[2] == 1
	fa.offset = uint32(data[3])<<24 + uint32(data[4])<<16 + uint32(data[5])<<8 + uint32(data[6])
	fa.data = data[7:]
}
func (fe *Fetch) toByte() []byte {
	var data [2]byte
	data[0] = byte(fe.partitionID >> 8)
	data[1] = byte(fe.partitionID & 0xFF)
	return data[0:2]
}

func (fe *Fetch) fromByte(data []byte) {
	fe.partitionID = uint16(data[0])<<8 + uint16(data[1])
}

func (a *Assignment) toByte() []byte {
	res := make([]byte, 1+len(a.Partitions)*2)
	res[0] = byte(len(a.Partitions))
	for i, partitionID := range a.Partitions {
		pos := 1 + i*2
		res[pos] = byte(partitionID >> 8)
		res[pos+1] = byte(partitionID & 0xFF)
	}
	return res
}

func (a *Assignment) fromByte(data []byte) {
	res := make([]uint16, data[0])
	for i := 0; i < int(data[0]); i++ {
		res[i] = uint16(data[1+i*2])<<8 + uint16(data[i+i*2+1])
	}
	a.Partitions = res
}

func (pr *ProducerRegister) toByte() []byte { // toByte dùng để chuyển struct thành mảng byte để gửi đi qua stream
	// 2 byte đầu tiên là port
	// 2 byte tiếp theo là topic ID
	// cái 0xFF có giá trị nhị phân là 11111111, khi 1 số and với 0xFF sẽ lấy ra 8 bit cuối cùng của số đó
	var data [4]byte
	data[0] = byte(pr.port >> 8)   // shift right 8 bits để lấy 8 bit đầu tiên của port và lưu vào data[0]
	data[1] = byte(pr.port & 0xFF) // lấy and 0xFF để lấy 8 bit cuối cùng của port và lưu vào data[1]

	data[2] = byte(pr.topicID >> 8)   // tương tự như trên, shift right 8 bits để lấy 8 bit đầu tiên của topicID và lưu vào data[2]
	data[3] = byte(pr.topicID & 0xFF) // lấy and 0xFF để lấy 8 bit cuối cùng của topicID và lưu vào data[3]
	return data[0:4]
}

func (pr *ProducerRegister) fromByte(data []byte) { // fromByte thì dùng để chuyển mảng byte thành struct ProducerRegister
	pr.port = uint16(data[0])<<8 + uint16(data[1])    // shift left 8 bits để đưa 8 bit đầu về đúngv vị trí và cộng với 8 bit cuối để lấy ra giá trị port
	pr.topicID = uint16(data[2])<<8 + uint16(data[3]) // shift left 8 bits để đưa 8 bit đầu về đúng vị trí và cộng với 8 bit cuối để lấy ra giá trị topicID
}

type ConsumerRegister struct {
	port    uint16
	topicID uint16
	groupID uint16
}

func (cr *ConsumerRegister) toByte() []byte {
	var data [6]byte
	data[0] = byte(cr.port >> 8)   // shift right 8 bits để lấy 8 bit đầu tiên của port và lưu vào data[0]
	data[1] = byte(cr.port & 0xFF) // lấy and 0xFF để lấy 8 bit cuối cùng của port và lưu vào data[1]

	data[2] = byte(cr.topicID >> 8)   // tương tự như trên, shift right 8 bits để lấy 8 bit đầu tiên của topicID và lưu vào data[2]
	data[3] = byte(cr.topicID & 0xFF) // lấy and 0xFF để lấy 8 bit cuối cùng của topicID và lưu vào data[3]

	data[4] = byte(cr.groupID >> 8)   // tương tự như trên, shift right 8 bits để lấy 8 bit đầu tiên của groupID và lưu vào data[4]
	data[5] = byte(cr.groupID & 0xFF) // lấy and 0xFF để lấy 8 bit cuối cùng của groupID và lưu vào data[5]
	return data[0:6]
}

func (cr *ConsumerRegister) fromByte(data []byte) {
	cr.port = uint16(data[0])<<8 + uint16(data[1])    // shift left 8 bits để đưa 8 bit đầu về đúngv vị trí và cộng với 8 bit cuối để lấy ra giá trị port
	cr.topicID = uint16(data[2])<<8 + uint16(data[3]) // shift left 8 bits để đưa 8 bit đầu về đúng vị trí và cộng với 8 bit cuối để lấy ra giá trị topicID
	cr.groupID = uint16(data[4])<<8 + uint16(data[5]) // shift left 8 bits để đưa 8 bit đầu về đúng vị trí và cộng với 8 bit cuối để lấy ra giá trị groupID
}
func readFromStream(stream_rd *bufio.ReadWriter) ([]byte, error) {
	var err error
	header, err := stream_rd.ReadByte()
	if err != nil {
		return nil, err
	}
	data, err := stream_rd.Peek(int(header))
	if err != nil {
		return nil, err
	}
	_, err = stream_rd.Discard(int(header))
	if err != nil {
		return nil, err
	}
	return data, err
}

func parseMessage(stream_message []byte) *Message {
	if len(stream_message) == 0 {
		return nil
	}
	switch stream_message[0] {

	case ECHO: // Sau khi nhận được byte đầu tiên là ECHO sẽ cắt từ byte thứ 2 trở đi
		// convert và tạo 1 Message mới với ECHO là string từ byte thứ 2 trở đi
		var st = string(stream_message[1:])
		return &Message{ECHO: &st}
	case COMMIT_OFFSET:
		var co = &CommitOffset{}
		co.TopicID = uint16(stream_message[1])<<8 + uint16(stream_message[2])
		co.GroupID = uint16(stream_message[3])<<8 + uint16(stream_message[4])
		co.Offset = uint32(stream_message[5])<<24 + uint32(stream_message[6])<<16 + uint32(stream_message[7])<<8 + uint32(stream_message[8])
		return &Message{COMMIT_OFFSET: co}
	case RESPONSE_ECHO:
		var st = string(stream_message[1:])
		return &Message{RESPONSE_ECHO: &st}

	case PRODUCER_REGISTER:
		var pr = &ProducerRegister{}
		pr.fromByte(stream_message[1:])
		return &Message{
			PRODUCER_REGISTER: pr,
		}
	case RESPONSE_PRODUCER_REGISTER:
		var st = stream_message[1]
		return &Message{RESPONSE_PRODUCER_REGISTER: &st}
	case PCM:
		return &Message{PCM: stream_message[1:]}
	case R_PCM:
		var st = stream_message[1]
		return &Message{R_PCM: &st}
	case CONSUMER_REGISTER:
		var cr = &ConsumerRegister{}
		cr.fromByte(stream_message[1:])
		return &Message{
			CONSUMER_REGISTER: cr,
		}
	case ASSIGNMENT_ACK:
		var st = stream_message[1]
		return &Message{ASSIGNMENT_ACK: &st}
	case RESPONSE_CONSUMER_REGISTER:
		var st = stream_message[1]
		return &Message{RESPONSE_CONSUMER_REGISTER: &st}
	case ASSIGNMENT:
		var ass = &Assignment{}
		ass.fromByte(stream_message[1:])
		return &Message{ASSIGNMENT: ass}
	case FETCH:
		var fe = &Fetch{}
		fe.fromByte(stream_message[1:])
		return &Message{FETCH: fe}
	case FETCH_ACK:
		var fa = &FetchAck{}
		fa.fromByte(stream_message[1:])
		return &Message{FETCH_ACK: fa}
	default:
		return nil
	}
}

func ReadMessageFromStream(stream_rd *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(stream_rd)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

func writeDataToStreamWithType(stream_wt *bufio.ReadWriter, messageType byte, data string) error {
	var err error
	// Write size
	err = stream_wt.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Write type
	err = stream_wt.WriteByte(messageType)
	if err != nil {
		return err
	}
	// Write data
	_, err = stream_wt.WriteString(string(data))
	if err != nil {
		return err
	}
	err = stream_wt.Flush()
	if err != nil {
		return err
	}
	return nil
}

type byteSerializable interface {
	toByte() []byte
}

func WriteSerializableToStream(stream_wt *bufio.ReadWriter, messageType byte, data byteSerializable) error {
	return writeDataToStreamWithType(stream_wt, messageType, string(data.toByte()))
}

func WriteMessageCommitOffsetToStream(stream_wt *bufio.ReadWriter, messageType byte, data *CommitOffset) error {
	var coData [8]byte
	coData[0] = byte(data.TopicID >> 8)
	coData[1] = byte(data.TopicID & 0xFF)
	coData[2] = byte(data.GroupID >> 8)
	coData[3] = byte(data.GroupID & 0xFF)
	coData[4] = byte(data.Offset >> 24)
	coData[5] = byte((data.Offset >> 16) & 0xFF)
	coData[6] = byte((data.Offset >> 8) & 0xFF)
	coData[7] = byte(data.Offset & 0xFF)
	if err := writeDataToStreamWithType(stream_wt, COMMIT_OFFSET, string(coData[:])); err != nil {
		return err
	}
	return nil
}

func WriteMessageToStream(stream_wt *bufio.ReadWriter, message *Message) error {
	if message.ECHO != nil {
		if err := writeDataToStreamWithType(stream_wt, ECHO, *message.ECHO); err != nil {
			return err
		}
	}
	if message.RESPONSE_ECHO != nil {
		if err := writeDataToStreamWithType(stream_wt, RESPONSE_ECHO, *message.RESPONSE_ECHO); err != nil {
			return err
		}
	}
	if message.RESPONSE_PRODUCER_REGISTER != nil {
		data := fmt.Sprintf("%d", *message.RESPONSE_PRODUCER_REGISTER)
		if err := writeDataToStreamWithType(stream_wt, RESPONSE_PRODUCER_REGISTER, data); err != nil {
			return err
		}
	}
	if message.PCM != nil {
		data := string(message.PCM)
		if err := writeDataToStreamWithType(stream_wt, PCM, data); err != nil {
			return err
		}
	}
	if message.R_PCM != nil {
		data := fmt.Sprintf("%d", *message.R_PCM)
		if err := writeDataToStreamWithType(stream_wt, R_PCM, data); err != nil {
			return err
		}
	}
	if message.RESPONSE_CONSUMER_REGISTER != nil {
		data := fmt.Sprintf("%d", *message.RESPONSE_CONSUMER_REGISTER)
		if err := writeDataToStreamWithType(stream_wt, RESPONSE_CONSUMER_REGISTER, data); err != nil {
			return err
		}
	}
	if message.ASSIGNMENT != nil {
		if err := WriteSerializableToStream(stream_wt, ASSIGNMENT, message.ASSIGNMENT); err != nil {
			return err
		}
	}
	if message.ASSIGNMENT_ACK != nil {
		data := fmt.Sprintf("%d", *message.ASSIGNMENT_ACK)
		if err := writeDataToStreamWithType(stream_wt, ASSIGNMENT_ACK, data); err != nil {
			return err
		}
	}
	if message.FETCH != nil {
		if err := WriteSerializableToStream(stream_wt, FETCH, message.FETCH); err != nil {
			return err
		}
	}
	if message.FETCH_ACK != nil {
		if err := WriteSerializableToStream(stream_wt, FETCH_ACK, message.FETCH_ACK); err != nil {
			return err
		}
	}
	return nil
}
