package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	fmt.Println(os.Args)

	// os.Args[0] is the name of the program(os.Args vị trí 0 là tên của chương trình )
	// so we need at least 2 arguments to run as server or client. (Nên chúng ta cần lấy từ vị trí 1 trở đi để biết cần chạy server hay client)
	// Run as server if the first argument is "server", otherwise run as client. (Khởi động server nếu tham số đầu tiên là "server", ngược lại chạy như client)
	// trong trường hợp tham số là client thì cần thêm 1 tham số nữa là port để biết client sẽ kết nối đến port nào

	switch os.Args[1] {
	case "broker":
		broker := &Broker{}
		broker.init()
		err := broker.StartBrokerServer()
		if err != nil {
			fmt.Println("Error starting broker server: ", err)
		}
	case "producer":
		port, err := strconv.ParseInt(os.Args[2], 10, 32)
		topicID, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			panic(err)
		}
		producer := &Producer{
			port:    uint16(port),
			topicID: uint16(topicID),
		}
		producer.StartProducerServer()
	case "consumer":
		port, err := strconv.ParseInt(os.Args[2], 10, 32)
		topicID, err := strconv.ParseInt(os.Args[3], 10, 32)
		groupID, err := strconv.ParseInt(os.Args[4], 10, 32)
		if err != nil {
			panic(err)
		}
		consumer := &Consumer{
			port:    uint16(port),
			topicID: uint16(topicID),
			groupID: uint16(groupID),
		}
		consumer.StartConsumerServer()
	}
}
