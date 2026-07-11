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
	// other message types can be added here

	// ACK
	RESPONSE_ECHO              = 101
	RESPONSE_PRODUCER_REGISTER = 102
	R_PCM                      = 103
)

type Message struct {
	ECHO              *string
	PRODUCER_REGISTER *ProducerRegister
	PCM               []byte
	// other message types can be added here

	// ACK
	RESPONSE_ECHO              *string
	RESPONSE_PRODUCER_REGISTER *byte
	R_PCM                      *byte
}

type ProducerRegister struct {
	port    uint16
	topicID uint16
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

func WriteMessageRegisterToStream(stream_wt *bufio.ReadWriter, messageType byte, data *ProducerRegister) error {
	prData := data.toByte()
	if err := writeDataToStreamWithType(stream_wt, PRODUCER_REGISTER, string(prData)); err != nil {
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
	return nil
}
