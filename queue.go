package main

import (
	"fmt"
)

const (
	ASLOT     = 255   // mỗi slot có 255 byte dữ liệu
	SLOT_SIZE = 10000 // tổng số slot là 10000 slot
)

var ArrMQ [ASLOT * SLOT_SIZE]byte  // mảng lưu trữ message queue, tổng cộng có 10000 slot , mỗi slot có 255 byte dữ liệu
var SizeMQ [ASLOT * SLOT_SIZE]byte // mảng lưu trữ kích thước của message queue cho mỗi slot

type Queue struct {
	head       uint32
	tail       uint32
	count      uint32
	baseOffset uint64
}

func (q *Queue) init() {
	q.head = 0
	q.tail = 0
	q.count = 0
	q.baseOffset = 0
}

func (q *Queue) push(data []byte) {
	copy(ArrMQ[q.tail:q.tail+uint32(len(data))], data)
	SizeMQ[q.tail] = byte(len(data))
	q.count++
	q.tail += 255
	q.tail %= ASLOT * SLOT_SIZE
}

func (q *Queue) pop() []byte {
	if q.count == 0 {
		return nil
	}
	data := ArrMQ[q.head : q.head+uint32(SizeMQ[q.head])]
	q.count--
	q.head += 255
	q.head %= ASLOT * SLOT_SIZE
	q.baseOffset += 1
	return data
}

func (q *Queue) peek(offset uint) []byte {
	if offset >= uint(q.count) {
		return nil
	}
	posision := q.head + uint32(offset)*ASLOT
	posision %= ASLOT * SLOT_SIZE
	data := ArrMQ[posision : posision+uint32(SizeMQ[posision])]
	return data
}

func (q *Queue) debug() {
	cur := q.head
	fmt.Print("Debug Message Queue: \n")
	for {
		data := ArrMQ[cur : cur+uint32(SizeMQ[cur])]
		fmt.Printf("%s", string(data))
		cur += 255
		cur %= ASLOT * SLOT_SIZE
		if cur == q.tail {
			break
		}
	}
}
