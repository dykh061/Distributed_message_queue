package main

import (
	"fmt"
	"sync"
)

const (
	ASLOT     = 255   // mỗi slot có 255 byte dữ liệu
	SLOT_SIZE = 10000 // tổng số slot là 10000 slot
)

type Queue struct {
	mu         sync.RWMutex
	arr        [ASLOT * SLOT_SIZE]byte // mảng lưu trữ message queue, tổng cộng có 10000 slot , mỗi slot có 255 byte dữ liệu
	Size       [ASLOT * SLOT_SIZE]byte // mảng lưu trữ kích thước của message queue cho mỗi slot
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
	q.mu.Lock()
	defer q.mu.Unlock()
	copy(q.arr[q.tail:q.tail+uint32(len(data))], data) // copy dữ liệu data vào mảng arr tại vị trí tail
	q.Size[q.tail] = byte(len(data))
	q.count++
	q.tail += 255
	q.tail %= ASLOT * SLOT_SIZE
}

func (q *Queue) getCount() uint32 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.count
}
func (q *Queue) pop() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return nil
	}
	size := uint32(q.Size[q.head]) // lấy kích thước của message queue tại vị trí head
	data := make([]byte, size)
	copy(data, q.arr[q.head:q.head+size])
	q.count--
	q.head += 255
	q.head %= ASLOT * SLOT_SIZE
	q.baseOffset += 1
	return data
}

func (q *Queue) peek(offset uint) []byte {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if offset >= uint(q.count) {
		return nil
	}
	posision := q.head + uint32(offset)*ASLOT
	posision %= ASLOT * SLOT_SIZE
	size := uint32(q.Size[posision])
	data := make([]byte, size)
	copy(data, q.arr[posision:posision+size])
	return data
}

func (q *Queue) peekLocked(offset uint) []byte { // hàm này được dùng khi đã lock mutex bên ngoài, mục đích tránh deadlock
	if offset >= uint(q.count) {
		return nil
	}
	posision := q.head + uint32(offset)*ASLOT
	posision %= ASLOT * SLOT_SIZE
	size := uint32(q.Size[posision])
	data := make([]byte, size)
	copy(data, q.arr[posision:posision+size])
	return data
}

func (q *Queue) debug() {
	q.mu.RLock()
	defer q.mu.RUnlock()
	cur := q.head
	fmt.Print("Debug Message Queue: \n")
	for {
		data := q.arr[cur : cur+uint32(q.Size[cur])]
		fmt.Printf("%s", string(data))
		cur += 255
		cur %= ASLOT * SLOT_SIZE
		if cur == q.tail {
			break
		}
	}
}

func (q *Queue) fetchMessage(maxMessages uint16, offset uint32) (data []byte, nextOffset uint32, found bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.count == 0 {
		return nil, offset, false
	}

	// Ví dụ base = 100, count = 3 → log chứa 100, 101, 102.
	// maxLogicOffset = 103.
	maxLogicOffset := q.baseOffset + uint64(q.count)

	// offset nhỏ hơn baseOffset: record đã bị retention xoá.
	// offset >= maxLogicOffset: chưa có record mới.
	if uint64(offset) < q.baseOffset || uint64(offset) >= maxLogicOffset {
		return nil, offset, false
	}

	remaining := maxLogicOffset - uint64(offset)
	readCount := uint64(maxMessages)
	if readCount > remaining {
		readCount = remaining
	}
	nextOffset = offset
	var messages []byte

	for i := uint64(0); i < readCount; i++ {
		relativeOffset := nextOffset - uint32(q.baseOffset)
		msg := q.peekLocked(uint(relativeOffset))
		lengthmsg := uint16(len(msg))
		first := byte(lengthmsg >> 8)
		last := byte(lengthmsg & 0xff)
		messages = append(messages, first, last)
		messages = append(messages, msg...)
		nextOffset++
	}
	return messages, nextOffset, true
}
