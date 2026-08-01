package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

type Producer struct {
	port    uint16
	topicID uint16
}

// Kết nối tới Broker.
// Gửi message PRODUCER_REGISTER chứa port mà Producer đang lắng nghe.
// Chờ Broker phản hồi ACK đăng ký.
// Sau khi hàm kết thúc thì kết nối này sẽ được đóng.
func (producer *Producer) registerWithBroker() error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		return fmt.Errorf("cannot connect to broker on port %d: %w", BROKER_PORT, err)
	}
	//

	defer conn.Close()
	stream_rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	message := Message{
		PRODUCER_REGISTER: &ProducerRegister{
			port:    producer.port,
			topicID: producer.topicID,
		},
	}

	err = WriteSerializableToStream(stream_rw, PRODUCER_REGISTER, message.PRODUCER_REGISTER)
	if err != nil {
		return err
	}

	resp, err := ReadMessageFromStream(stream_rw)
	if err != nil {
		return err
	}
	if resp.RESPONSE_PRODUCER_REGISTER == nil {
		fmt.Println("Broker did not send register ack")
	} else {
		fmt.Printf("Receive response from broker: %v\n", *resp.RESPONSE_PRODUCER_REGISTER)
	}
	return nil
}

// Khởi động TCP Server của Producer.
// Đăng ký port của Producer với Broker.
// Sau khi Broker kết nối ngược lại vào port này,
// Producer sẽ gửi và nhận message thông qua kết nối đó.
func (producer *Producer) StartProducerServer() error {
	var err error

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", producer.port))
	if err != nil {
		return err
	}
	// connect to broker to send register
	err = producer.registerWithBroker()
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
	rd := bufio.NewReader(os.Stdin)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			break
		}
		err = WriteMessageToStream(
			stream_rw,
			&Message{PCM: []byte(line)},
		)
		if err != nil {
			break
		}
		resp, err := ReadMessageFromStream(stream_rw)
		if err != nil {
			break
		}
		fmt.Printf("Receive Message From Broker: %d\n", *resp.R_PCM)
	}
	return err
}
