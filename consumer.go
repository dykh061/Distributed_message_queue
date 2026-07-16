package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

type Consumer struct {
	port    uint16
	topicID uint16
	groupID uint16
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

	err = WriteMessageConsumerRegisterToStream(stream_rw, CONSUMER_REGISTER, message.CONSUMER_REGISTER)
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
			break
		}
		if resp != nil {
			var ack = byte(1) // ACK
			str := string(resp.PCM)
			fmt.Printf("Consumer received message: %s\n", str)
			err := WriteMessageToStream(stream_rw, &Message{RESPONSE_CONSUMER_REGISTER: &ack})
			if err != nil {
				log.Println(err)
				break
			}
		} else {
			time.Sleep(5 * time.Second)
			fmt.Println("No message received, waiting for 5 seconds before checking again...")
		}
	}
	return err
}
