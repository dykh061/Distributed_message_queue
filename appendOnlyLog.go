package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrLogClosed = errors.New("log is closed")
var ErrOffsetNotFound = errors.New("offset not found")

type segment struct {
	file       *os.File      //  File vật lý của segment này trên disk.
	writer     *bufio.Writer // vùng đệm ghi dữ liệu
	positions  []int64       // vị trí byte bắt đầu của mỗi message trong file
	baseOffset uint64
	// baseOffset không lưu "vị trí", mà lưu offset đầu tiên của segment đó trong toàn bộ partition.
	// Ví dụ partition có giới hạn 100 message/segment.
	// thì cái segment đầu tiên baseOffset =0, cái segment thứ 2 baseOffset = 100, cái segment thứ 3 baseOffset = 200
	size     int64 // kích thước hiện tại của segment
	syncSize int64 // kích thước đã được sync xuống đĩa vật lý
}
type AppendOnlyLog struct {
	mu         sync.RWMutex
	dir        string     //Đường dẫn tới DIRECTORY chứa tất cả segment của partition này.
	segments   []*segment // danh sách tất cả segment của partition này
	active     *segment   // segment hiện tại đang nhận dữ liệu mới
	nextOffset uint64

	closed bool // true nếu log đã bị đóng, false nếu log vẫn còn mở
	//  Logical offset sẽ được cấp cho MESSAGE TIẾP THEO. Đây không phải vị trí byte mà là vị trí số
	// for example đang có offset 1 2 thì nextOffset kế tiếp là 3 là vị trí số của message tiếp theo
	maxSegmentSize int64 //Kích thước tối đa của MỘT segment, tính bằng BYTE.

	// Quản lý background flusher
	stopFlusher chan struct{} // channel để báo cho background flusher dừng lại cho luồng chính dùng
	flusherDone chan struct{} // channel để gourutine báo cho luồng chính biết là đã dừng xong
	// dùng struct vì struct{}{} không tốn bộ nhớ 0 byte, còn nếu dùng bool thì sẽ tốn 1 byte
	// 2 channel này chỉ đóng vai trò bộ đàm để liên lạc cho 2 luồng
	flushErr error // lưu trữ lỗi nếu có xảy ra trong background flusher
	stopOnce sync.Once
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
			// HasSuffix kiểm tra xem tên file có kết thúc bằng Suffix = ".log" không
			logFiles = append(logFiles, entry.Name())
		}
	}

	sort.Strings(logFiles)
	// Vì tên file là zero-padded 12 chữ số bao gồm baseOffset trong đó mà baseOffset là offset bắt đầu của 1 segment  (000000000000.log),
	// sắp xếp alphabet = sắp xếp theo baseOffset tăng dần.

	if len(logFiles) == 0 { // nếu chưa có file log nào thì tạo segment mới
		segment, err := log.createSegment(0)
		if err != nil {
			return nil, err
		}

		log.segments = append(log.segments, segment)
		log.active = segment

		log.startBackgroundFlusher(50 * time.Millisecond)
		return log, nil
	}

	// nếu có file log thì dựng lại từng segment
	for _, filename := range logFiles {
		// Parse baseOffset từ tên file: "000000000150.log" → 150
		baseOffset, err := parseBaseOffset(filename)
		if err != nil {
			return nil, err
		}
		filepath := fmt.Sprintf("%s/%s", dir, filename) // dựng lại đường dẫn đầy đủ tới file log
		file, err := os.OpenFile(filepath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		seg := &segment{
			file:       file,
			writer:     nil, // các segment cũ không cần writer vì chỉ đọc dữ liệu, segment active mới cần writer
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
		seg.syncSize = seg.size                  // khi recover xong thì syncSize = size vì lúc này dữ liệu đã được ghi xuống đĩa vật lý hết
		log.segments = append(log.segments, seg) // có thêm segment vào danh sách khi for hết file thì segments sẽ có đủ tất cả segment

		// Cập nhật nextOffset = baseOffset + số message đã recover trong segment
		log.nextOffset = baseOffset + uint64(len(seg.positions)) // có nextOffset còn thiếu active
	}
	// 5. Segment cuối cùng là active
	log.active = log.segments[len(log.segments)-1] // có đủ active

	// nếu segment củ được load từ file lên nó chưa có writer nên phải tạo writer mới cho nó để ghi dữ liệu tiếp theo
	if log.active.writer == nil {
		log.active.writer = bufio.NewWriterSize(log.active.file, 64*1024) // 64KB buffer
	}

	log.startBackgroundFlusher(50 * time.Millisecond)
	return log, nil
}

// recoverSegment đọc file segment từ đầu đến cuối, dựng lại positions[].
// Nếu gặp record bị ghi dở (partial write do crash), cắt bỏ phần dở.
func recoverSegment(seg *segment) error {
	// file.Stat() lấy cấc thông tin
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
			if err := seg.file.Truncate(position); err != nil { // cắt file tại vị trí position, xóa bỏ phần dở phía sau
				return err
			}
			break
		}
		lengthBytes := make([]byte, 4)
		_, err := seg.file.ReadAt(lengthBytes, position) // đọc 4 byte từ file tại vị trí position ; tương ứng với 4 byte chứa lenght của message
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(lengthBytes) // chuyển 4 byte đó thành số nguyên uint32 để biết độ dài của message
		if length == 0 {
			// Record có length = 0 là bất hợp lệ, cắt bỏ
			if err := seg.file.Truncate(position); err != nil {
				return err
			}
			break
		}
		recordEnd := position + 4 + int64(length) // xác định vị trí byte kết thúc của record hiện tại
		if recordEnd > fileSize {
			// Payload bị ghi dở → cắt bỏ record này
			// Ví dụ: length nói payload 100 byte nhưng file chỉ còn 50 byte
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
	// do position = recordEnd nên là position luôn đại diện cho vị trí bắt đầu của record tiếp theo,
	// hoặc điểm kết thúc của phần dữ liệu hợp lệ. Vì vậy sau khi scan xong, seg.size = position.
	return nil
}

// parseBaseOffset chuyển tên file "000000000150.log" → uint64(150)
func parseBaseOffset(filename string) (uint64, error) {
	name := strings.TrimSuffix(filename, ".log") // Cắt bỏ đuôi ".log"
	return strconv.ParseUint(name, 10, 64)       // chuyển file name  từ hệ thập phân sang uint64
}

func (log *AppendOnlyLog) createSegment(baseOffset uint64) (*segment, error) {
	filename := fmt.Sprintf(
		"%s/%012d.log", // %012d : %d số nguyên tổng cộng 12 chữ số nếu không đủ thì thêm 0 vào trước ví dụ baseOffset = 123 thì filename = 000000000123.log
		log.dir,
		baseOffset,
	)
	// kết quả của dòng này sẽ là ví dụ : D:/kafka/topic-1/partition-0/000000000123.log

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
		writer:     bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		positions:  make([]int64, 0),
		baseOffset: baseOffset,
		size:       0,
	}, nil
}

func (log *AppendOnlyLog) rollSegment() error { // tạo segment mới khi segment hiện tại đã đầy
	if log.active != nil && log.active.writer != nil {
		err := log.active.writer.Flush()
		if err != nil {
			return err
		}
		err = log.active.file.Sync()
		if err != nil {
			return err
		}
		log.active.writer = nil // giải phóng RAM buffer 64KB của segment cũ
	}

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
	log.stopBackgroundFlusher() // dừng background flusher trước khi đóng file
	log.mu.Lock()
	defer log.mu.Unlock()

	if log.closed {
		return nil
	}
	log.closed = true // đánh dấu log đã đóng để các goroutine khác không được phép ghi dữ liệu nữa
	var errs []error
	if log.flushErr != nil {
		errs = append(errs, fmt.Errorf("background flush error: %w", log.flushErr))
	}
	for _, seg := range log.segments {
		err := seg.file.Close()
		errs = append(errs, err)
	}
	return errors.Join(errs...) // errors.Join gom tất cả các lỗi trong slice errs thành 1 lỗi duy nhất ngăn cách bởi \n
}

// ghi dữ liệu vào segment hiện tại
func (log *AppendOnlyLog) append(data []byte) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if log.closed {
		return 0, ErrLogClosed
	}

	if log.flushErr != nil {
		return 0, fmt.Errorf("background flush error: %w", log.flushErr)
	}
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
	n, err := log.active.writer.Write(record)
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

func (log *AppendOnlyLog) fetchOneLocked(offset uint64) ([]byte, error) {

	segment, err := log.findSegmentLocked(offset)
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

func (log *AppendOnlyLog) findSegmentLocked(offset uint64) (*segment, error) {
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
	if log.closed {
		log.mu.RUnlock()
		return nil, ErrLogClosed
	}
	//Kiểm tra nhanh bằng RLock xem có cần Flush active segment không
	needflush := (log.active != nil && log.active.writer != nil && offset+uint64(maxMessages) > log.active.baseOffset && log.active.writer.Buffered() > 0)
	log.mu.RUnlock()

	if needflush {
		log.mu.Lock()
		// Double-check: kiểm tra lại xem trong lúc chờ lock, goroutine khác đã flush chưa
		if log.active != nil && log.active.writer != nil && log.active.writer.Buffered() > 0 {
			if err := log.active.writer.Flush(); err != nil {
				log.mu.Unlock()
				return nil, err
			}
		}
		log.mu.Unlock() // nhả lock
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	messages := make([][]byte, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		data, err := log.fetchOneLocked(offset + uint64(i))
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

func (log *AppendOnlyLog) startBackgroundFlusher(interval time.Duration) {
	log.stopFlusher = make(chan struct{})
	log.flusherDone = make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(log.flusherDone)
		flushActive := func() {
			// tạo ra 1 biến flushActive chứa 1 func để gọi flushActive() khi ticker hoặc stopFlusher channel nhận được sự kiện
			// flush / write / và sync khác nhau thế nào:
			// wrrite chỉ là ghi vào buffer của go
			// flush là đẩy từ ram project xuống ram hệ điều hành (OS Page Cache)
			// sync là đẩy từ ram hệ điều hành xuống đĩa vật lý
			log.mu.Lock()
			defer log.mu.Unlock()

			// nếu không có active segment hoặc không có writer thì không cần flush
			if log.active == nil || log.active.writer == nil {
				return
			}

			if err := log.active.writer.Flush(); err != nil {
				log.flushErr = err
				return
			}

			if log.active.size > log.active.syncSize {
				// Đẩy từ OS Page Cache xuống đĩa vật lý
				if err := log.active.file.Sync(); err == nil {
					log.active.syncSize = log.active.size
				} else {
					log.flushErr = err
				}
			}

		}
		for { // vòng lặp đứng chờ sự kiện từ ticker hoặc stopFlusher channel
			select {
			case <-ticker.C: // mỗi khi đủ thời gian interval thì gọi flushActive() để flush dữ liệu xuống đĩa
				flushActive()
			case <-log.stopFlusher: // nếu chương trình dừng lại thì gọi flushActive() để flush dữ liệu xuống đĩa trước khi dừng
				flushActive()
				return
			}
		}
	}()
}

func (log *AppendOnlyLog) stopBackgroundFlusher() {
	log.stopOnce.Do(func() {
		if log.stopFlusher != nil {
			close(log.stopFlusher)
			<-log.flusherDone
		}
	})
}
