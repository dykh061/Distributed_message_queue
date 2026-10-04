package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// const (
// 	ASLOT     = 255   // mỗi slot có 255 byte dữ liệu
// 	SLOT_SIZE = 10000 // tổng số slot là 10000 slot
// )

// type Queue struct {
// 	mu         sync.RWMutex
// 	arr        [ASLOT * SLOT_SIZE]byte // mảng lưu trữ message queue, tổng cộng có 10000 slot , mỗi slot có 255 byte dữ liệu
// 	Size       [ASLOT * SLOT_SIZE]byte // mảng lưu trữ kích thước của message queue cho mỗi slot
// 	head       uint32
// 	tail       uint32
// 	count      uint32
// 	baseOffset uint64
// }

// func (q *Queue) init() {
// 	q.head = 0
// 	q.tail = 0
// 	q.count = 0
// 	q.baseOffset = 0
// }

// func (q *Queue) push(data []byte) {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	copy(q.arr[q.tail:q.tail+uint32(len(data))], data) // copy dữ liệu data vào mảng arr tại vị trí tail
// 	q.Size[q.tail] = byte(len(data))
// 	q.count++
// 	q.tail += 255
// 	q.tail %= ASLOT * SLOT_SIZE
// }

// func (q *Queue) getCount() uint32 {
// 	q.mu.RLock()
// 	defer q.mu.RUnlock()
// 	return q.count
// }
// func (q *Queue) pop() []byte {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	if q.count == 0 {
// 		return nil
// 	}
// 	size := uint32(q.Size[q.head]) // lấy kích thước của message queue tại vị trí head
// 	data := make([]byte, size)
// 	copy(data, q.arr[q.head:q.head+size])
// 	q.count--
// 	q.head += 255
// 	q.head %= ASLOT * SLOT_SIZE
// 	q.baseOffset += 1
// 	return data
// }

// func (q *Queue) peek(offset uint) []byte {
// 	q.mu.RLock()
// 	defer q.mu.RUnlock()
// 	if offset >= uint(q.count) {
// 		return nil
// 	}
// 	posision := q.head + uint32(offset)*ASLOT
// 	posision %= ASLOT * SLOT_SIZE
// 	size := uint32(q.Size[posision])
// 	data := make([]byte, size)
// 	copy(data, q.arr[posision:posision+size])
// 	return data
// }

// func (q *Queue) peekLocked(offset uint) []byte { // hàm này được dùng khi đã lock mutex bên ngoài, mục đích tránh deadlock
// 	if offset >= uint(q.count) {
// 		return nil
// 	}
// 	posision := q.head + uint32(offset)*ASLOT
// 	posision %= ASLOT * SLOT_SIZE
// 	size := uint32(q.Size[posision])
// 	data := make([]byte, size)
// 	copy(data, q.arr[posision:posision+size])
// 	return data
// }

// func (q *Queue) fetchMessage(maxMessages uint16, offset uint32) (data []byte, nextOffset uint32, found bool) {
// 	q.mu.RLock()
// 	defer q.mu.RUnlock()
// 	if q.count == 0 {
// 		return nil, offset, false
// 	}

// 	// Ví dụ base = 100, count = 3 → log chứa 100, 101, 102.
// 	// maxLogicOffset = 103.
// 	maxLogicOffset := q.baseOffset + uint64(q.count)

// 	// offset nhỏ hơn baseOffset: record đã bị retention xoá.
// 	// offset >= maxLogicOffset: chưa có record mới.
// 	if uint64(offset) < q.baseOffset || uint64(offset) >= maxLogicOffset {
// 		return nil, offset, false
// 	}

// 	remaining := maxLogicOffset - uint64(offset)
// 	readCount := uint64(maxMessages)
// 	if readCount > remaining {
// 		readCount = remaining
// 	}
// 	nextOffset = offset
// 	var messages []byte

// 	for i := uint64(0); i < readCount; i++ {
// 		relativeOffset := nextOffset - uint32(q.baseOffset)
// 		msg := q.peekLocked(uint(relativeOffset))
// 		lengthmsg := uint16(len(msg))
// 		first := byte(lengthmsg >> 8)
// 		last := byte(lengthmsg & 0xff)
// 		messages = append(messages, first, last)
// 		messages = append(messages, msg...)
// 		nextOffset++
// 	}
// 	return messages, nextOffset, true
// }

var ErrOffsetNotFound = errors.New("offset not found")

type segment struct {
	file       *os.File //  File vật lý của segment này trên disk.
	positions  []int64  // vị trí byte bắt đầu của mỗi message trong file
	baseOffset uint64
	// baseOffset không lưu "vị trí", mà lưu offset đầu tiên của segment đó trong toàn bộ partition.
	// Ví dụ partition có giới hạn 100 message/segment.
	// thì cái segment đầu tiên baseOffset =0, cái segment thứ 2 baseOffset = 100, cái segment thứ 3 baseOffset = 200
	size int64 // kích thước hiện tại của segment
}
type AppendOnlyLog struct {
	mu         sync.RWMutex
	dir        string     //Đường dẫn tới DIRECTORY chứa tất cả segment của partition này.
	segments   []*segment // danh sách tất cả segment của partition này
	active     *segment   // segment hiện tại đang nhận dữ liệu mới
	nextOffset uint64
	//  Logical offset sẽ được cấp cho MESSAGE TIẾP THEO. Đây không phải vị trí byte mà là vị trí số
	// for example đang có offset 1 2 thì nextOffset kế tiếp là 3 là vị trí số của message tiếp theo
	maxSegmentSize int64 //Kích thước tối đa của MỘT segment, tính bằng BYTE.
}

// permision của directory là 0755 => số 0 nói về số này nằm ở hệ bát phân
// 	r = read    = 4
// w = write   = 2
// x = execute = 1
// => 7 là full quyền
// owner - group - others
// owner có quyền 7
// group có quyền 5
// others có quyền

func OpenAppendOnlyLog(dir string) (*AppendOnlyLog, error) {
	err := os.MkdirAll(dir, 0755)

	if err != nil {
		return nil, err
	}
	log := &AppendOnlyLog{
		dir:            dir,
		segments:       make([]*segment, 0),
		nextOffset:     0,
		maxSegmentSize: 1024 * 1024 * 10, // 10MB
	}
	// đọc danh sách file trong directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// lọc ra file .log và sắp xếp
	var logFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			logFiles = append(logFiles, entry.Name())
		}
	}

	sort.Strings(logFiles)
	// Vì tên file là zero-padded 12 chữ số (000000000000.log),
	// sắp xếp alphabet = sắp xếp theo baseOffset tăng dần.

	if len(logFiles) == 0 {
		segment, err := log.createSegment(0)
		if err != nil {
			return nil, err
		}

		log.segments = append(log.segments, segment)
		log.active = segment

		return log, nil
	}

	// recoer từng segment
	for _, filename := range logFiles {
		// Parse baseOffset từ tên file: "000000000150.log" → 150
		baseOffset, err := parseBaseOffset(filename)
		if err != nil {
			return nil, err
		}
		filepath := fmt.Sprintf("%s/%s", dir, filename)
		file, err := os.OpenFile(filepath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		seg := &segment{
			file:       file,
			positions:  make([]int64, 0),
			baseOffset: baseOffset,
			size:       0,
		}
		// Đọc lại toàn bộ record trong file để dựng lại positions[]
		err = recoverSegment(seg) // khi recover thì danh sách position và size của segment sẽ được cập nhật lại còn thiếu nextOffset và active thôi
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		log.segments = append(log.segments, seg) // có thêm segment vào danh sách khi for hết file thì segments sẽ có đủ tất cả segment

		// Cập nhật nextOffset = baseOffset + số message đã recover trong segment
		log.nextOffset = baseOffset + uint64(len(seg.positions)) // có nextOffset còn thiếu active
	}
	// 5. Segment cuối cùng là active
	log.active = log.segments[len(log.segments)-1] // có đủ active
	return log, nil
}

// recoverSegment đọc file segment từ đầu đến cuối, dựng lại positions[].
// Nếu gặp record bị ghi dở (partial write do crash), cắt bỏ phần dở.
func recoverSegment(seg *segment) error {
	// lấy cấc thông tin
	// 	info.Size()      // kích thước file, tính bằng byte
	// info.Name()      // tên file
	// info.ModTime()   // thời gian sửa đổi
	// info.IsDir()     // có phải directory không
	info, err := seg.file.Stat()

	if err != nil {
		return err
	}

	fileSize := info.Size()
	var position int64
	for position < fileSize {
		// Cần ít nhất 4 byte cho length header
		if position+4 > fileSize {
			// Header bị ghi dở → cắt bỏ
			fmt.Printf("[recover] truncating partial header at byte %d\n", position)
			if err := seg.file.Truncate(position); err != nil { // cắt file tại vị trí position, xóa bỏ phần dở phía sau
				return err
			}
			break
		}
		// Đọc 4 byte length
		lengthBytes := make([]byte, 4)
		_, err := seg.file.ReadAt(lengthBytes, position) // đọc 4 byte từ file tại vị trí position
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(lengthBytes) // chuyển 4 byte đó thành số nguyên uint32 để biết độ dài của message
		if length == 0 {
			// Record có length = 0 là bất hợp lệ, cắt bỏ
			fmt.Printf("[recover] truncating zero-length record at byte %d\n", position)
			if err := seg.file.Truncate(position); err != nil {
				return err
			}
			break
		}
		recordEnd := position + 4 + int64(length) // xác định vị trí byte kết thúc của record hiện tại
		if recordEnd > fileSize {
			// Payload bị ghi dở → cắt bỏ record này
			// Ví dụ: length nói payload 100 byte nhưng file chỉ còn 50 byte
			fmt.Printf(
				"[recover] truncating partial record at byte %d (expected %d bytes, file has %d)\n",
				position, length, fileSize-position-4,
			)
			if err := seg.file.Truncate(position); err != nil {
				return err
			}
			break
		}
		// Record hợp lệ → ghi nhận vào positions
		seg.positions = append(seg.positions, position)
		position = recordEnd
	}
	seg.size = position
	// position luôn đại diện cho vị trí bắt đầu của record tiếp theo,
	// hoặc điểm kết thúc của phần dữ liệu hợp lệ. Vì vậy sau khi scan xong, seg.size = position.
	return nil
}

// parseBaseOffset chuyển tên file "000000000150.log" → uint64(150)
func parseBaseOffset(filename string) (uint64, error) {
	// Cắt bỏ đuôi ".log"
	name := strings.TrimSuffix(filename, ".log")
	return strconv.ParseUint(name, 10, 64)
}

func (log *AppendOnlyLog) createSegment(baseOffset uint64) (*segment, error) {
	filename := fmt.Sprintf(
		"%s/%012d.log", // %012d : %d số nguyên tổng cộng 12 chữ số nếu không đủ thì thêm 0 vào trước ví dụ baseOffset = 123 thì filename = 000000000123.log
		log.dir,
		baseOffset,
	)

	file, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		// file chưa tồn tại tạo mới
		// RDWR mở file cho cả đọc và ghi
		// APPEND mỗi lần ghi ghi vào cuối file
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &segment{
		file:       file,
		positions:  make([]int64, 0),
		baseOffset: baseOffset,
		size:       0,
	}, nil
}

func (log *AppendOnlyLog) rollSegment() error { // tạo segment mới khi segment hiện tại đã đầy

	newSegment, err := log.createSegment(log.nextOffset)
	// tạo segment mới với baseOffset = nextOffset ý là offset bắt đầu của segment mới sẽ là offset tiếp theo của segment này
	if err != nil {
		return err
	}
	log.segments = append(
		log.segments,
		newSegment,
	)

	// Segment mới trở thành active
	log.active = newSegment

	return nil
}

func (log *AppendOnlyLog) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, seg := range log.segments {
		_ = seg.file.Close()
	}
	return nil
}
func (log *AppendOnlyLog) append(data []byte) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	recordSize := int64(4 + len(data)) // 4 byte đầu tiên lưu kích thước của message, sau đó là dữ liệu message

	if log.active.size+recordSize > log.maxSegmentSize {
		// nếu segment hiện tại viết thêm recordSize vào mà đầy thì tạo segment mới
		err := log.rollSegment()
		if err != nil {
			return 0, err
		}
	}

	offset := log.nextOffset // lấy offset hiện tại để trả về cho message mới

	position := log.active.size // lấy vị trí byte hiện tại

	// Tạo record: 4 byte length + message data
	record := make([]byte, int(recordSize))

	// Ghi kích thước message vào 4 byte đầu tiên
	binary.BigEndian.PutUint32(record[:4], uint32(len(data)))

	// Ghi dữ liệu từ byte thứ 5 trở đi
	copy(record[4:], data)

	// Ghi toàn bộ record xuống file
	n, err := log.active.file.Write(record)
	if err != nil {
		return 0, err
	}
	if n != len(record) { // nếu số byte ghi xuống file không bằng kích thước record thì báo lỗi
		return 0, io.ErrShortWrite
	}
	// Lưu vị trí message
	log.active.positions = append(log.active.positions, position)

	// cập nhật kích thước segment và offset tiếp theo
	log.active.size += recordSize
	log.nextOffset++
	return offset, nil
}

func (log *AppendOnlyLog) fetch(offset uint64) ([]byte, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.fetchOne(offset)
}

func (log *AppendOnlyLog) fetchOne(offset uint64) ([]byte, error) {

	segment, err := log.findSegment(offset)
	if err != nil {
		return nil, err
	}
	index := offset - segment.baseOffset
	// tính toán vị trí của message trong segment hiện tại,
	// ví dụ segment có baseOffset = 100, offset = 102 thì index = 2, tức là message thứ 2 trong segment

	position := segment.positions[index]
	// láy vị trí byte bắt đầu của message trong file segment

	lengthBytes := make([]byte, 4)

	_, err = segment.file.ReadAt(lengthBytes, position)
	// đọc 4 byte chứa độ dài của message từ file segment tại vị trí position

	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBytes)
	// chuyển đổi 4 byte đó thành số nguyên uint32 để biết độ dài của message
	data := make([]byte, length)

	_, err = segment.file.ReadAt(data, position+4)
	// đọc dữ liệu message từ file segment tại vị trí position + 4 (vì 4 byte đầu là độ dài)

	if err != nil {
		return nil, err
	}
	return data, nil

}

func (log *AppendOnlyLog) findSegment(offset uint64) (*segment, error) {
	for i := len(log.segments) - 1; i >= 0; i-- {
		segment := log.segments[i]
		if segment.baseOffset <= offset {
			localIndex := offset - segment.baseOffset
			if localIndex >= uint64(len(segment.positions)) {
				return nil, ErrOffsetNotFound // offset vượt quá segment này
			}
			return segment, nil
		}
	}
	return nil, ErrOffsetNotFound
}

func (log *AppendOnlyLog) fetchBatch(offset uint64, maxMessages int) ([][]byte, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	messages := make([][]byte, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		data, err := log.fetchOne(offset + uint64(i))
		if err != nil {
			if errors.Is(err, ErrOffsetNotFound) {
				break
			}
			return messages, err
		}
		messages = append(messages, data)
	}
	return messages, nil
}
