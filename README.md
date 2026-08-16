# DistributedMQ

`DistributedMQ` là một project học tập viết bằng Go, mô phỏng các thành phần cốt lõi của một distributed message queue theo phong cách Kafka ở mức tối giản:

- Broker nhận message từ producer và phân phối vào partition.
- Topic được chia thành 3 partition cố định.
- Consumer group nhận partition assignment theo round-robin.
- Consumer fetch message theo offset, xử lý batch và commit offset về broker.
- Broker lưu committed offset theo từng `topic / group / partition` trong bộ nhớ.

Project được xây dựng để học về TCP protocol, topic/partition, consumer group, offset commit, rebalance và đồng bộ hoá state trong Go. Đây **không phải** Kafka-compatible broker và chưa phù hợp để dùng trong production.

## Mục lục

- [Kiến trúc](#kiến-trúc)
- [Yêu cầu và cách chạy](#yêu-cầu-và-cách-chạy)
- [Luồng xử lý](#luồng-xử-lý)
- [Offset và commit](#offset-và-commit)
- [TCP protocol](#tcp-protocol)
- [Cấu trúc source](#cấu-trúc-source)
- [Storage và giới hạn](#storage-và-giới-hạn)
- [Concurrency](#concurrency)
- [Giới hạn hiện tại](#giới-hạn-hiện-tại)

## Kiến trúc

```text
                     PRODUCER_REGISTER
Producer  ──────────────────────────────────> Broker :10000
    ^                                                |
    | broker dial ngược lại producer                 | PCM
    +───────────────────────────────────────────────> Topic
                                                      ├─ Partition 0 / Queue
                                                      ├─ Partition 1 / Queue
                                                      └─ Partition 2 / Queue

                     CONSUMER_REGISTER
Consumer  ──────────────────────────────────> Broker :10000
    ^                                                |
    | broker dial ngược lại consumer                 | ASSIGNMENT
    +────────────────────────────────────────────────+
    | FETCH / FETCH_ACK / COMMIT_OFFSET / COMMIT_OFFSET_ACK
    +────────────────────────────────────────────────+
```

Mỗi producer hoặc consumer mở một TCP listener riêng, sau đó đăng ký listener port đó với broker. Broker kết nối ngược lại vào port đã đăng ký và dùng kết nối thứ hai để truyền dữ liệu runtime.

| Thành phần | Vai trò |
|---|---|
| `Broker` | Lắng nghe đăng ký tại port `10000`, quản lý topic, partition và consumer group. |
| `Topic` | Có `topicID`, 3 partition cố định và danh sách consumer group. |
| `Partition` | Có `partitionID` và một queue in-memory riêng. |
| `Producer` | Đọc từng dòng từ `stdin`, gửi `PCM` vào topic đã đăng ký. |
| `Consumer` | Nhận assignment, fetch batch, xử lý message và commit offset. |
| `ConsumerGroup` | Lưu consumer tham gia group và committed offset theo partition. |

## Yêu cầu và cách chạy

### Yêu cầu

- Go `1.25.4` hoặc phiên bản tương thích với `go.mod`.
- Các port sử dụng được trên máy local.
- Chạy các lệnh từ thư mục gốc project.

### 1. Khởi động broker

```bash
go run . broker
```

Broker lắng nghe registration tại `:10000`.

### 2. Khởi động producer

```bash
go run . producer <port> <topicID>
```

Ví dụ:

```bash
go run . producer 10001 1
```

Sau khi broker kết nối ngược lại, nhập một dòng bất kỳ trong terminal producer để gửi message.

### 3. Khởi động consumer

```bash
go run . consumer <port> <topicID> <groupID>
```

Ví dụ:

```bash
go run . consumer 10002 1 10
```

Chạy thêm consumer cùng `topicID` và `groupID` để xem broker rebalance 3 partition:

```bash
go run . consumer 10003 1 10
```

| Số consumer trong group | Assignment |
|---:|---|
| 1 | Consumer 0: P0, P1, P2 |
| 2 | Consumer 0: P0, P2; Consumer 1: P1 |
| 3 | Consumer 0: P0; Consumer 1: P1; Consumer 2: P2 |

## Luồng xử lý

### Producer → broker

1. Producer mở listener tại port của nó.
2. Producer gửi `PRODUCER_REGISTER(port, topicID)` đến broker.
3. Broker tạo/tìm topic tương ứng và trả `RESPONSE_PRODUCER_REGISTER`.
4. Broker dial ngược lại producer.
5. Producer đọc từng dòng từ `stdin`, gửi `PCM`.
6. Broker chọn partition kế tiếp theo round-robin (`nextPartition`), push message vào queue và trả `R_PCM`.

### Consumer → broker

1. Consumer mở listener tại port của nó.
2. Consumer gửi `CONSUMER_REGISTER(port, topicID, groupID)`.
3. Broker tạo/tìm topic và consumer group, dial ngược lại consumer.
4. Broker thêm consumer vào group, rebalance và gửi `ASSIGNMENT`.
5. Consumer trả `ASSIGNMENT_ACK`, rồi gửi `FETCH(partitionID, offset)` cho các partition được assign.
6. Broker trả `FETCH_ACK` chứa batch message và `nextOffset`.
7. Consumer xử lý batch, gửi `COMMIT_OFFSET`.
8. Broker lưu offset cho group và trả `COMMIT_OFFSET_ACK`.
9. Khi ACK commit thành công, consumer fetch tiếp từ offset vừa được xác nhận.

## Offset và commit

Offset trong project dùng quy ước **next offset**:

```text
Đã xử lý message offset 0..9
COMMIT_OFFSET = 10
Lần FETCH kế tiếp bắt đầu từ offset 10
```

Consumer theo dõi ba dạng state:

| State | Ý nghĩa |
|---|---|
| `assignment[].offset` | Local position: offset mà consumer sẽ đọc/fetch tiếp. |
| `pendingCommit[partitionID]` | Offset đã gửi commit nhưng chưa nhận ACK. |
| `commitedOffset[]` | Offset broker đã xác nhận commit. Tên field hiện tại giữ nguyên theo source (`commitedOffset`). |

`COMMIT_OFFSET_ACK` mang `partitionID`, `offset` và `success`, giúp consumer ghép ACK với đúng commit đang chờ. Với một consumer quản lý nhiều partition, pending commit được lưu theo `partitionID`.

## TCP protocol

Mọi frame có format:

```text
+-----------+------+----------------+
| length: 1 | type | payload         |
+-----------+------+----------------+
```

- `length`: 1 byte, là kích thước của `type + payload`.
- `type`: 1 byte, xác định message type.
- `payload`: nhị phân, phụ thuộc vào type.
- Encoding số nguyên là big-endian thủ công.

### Message types

| Code | Request | Code | Response / ACK |
|---:|---|---:|---|
| 1 | `ECHO` | 101 | `RESPONSE_ECHO` |
| 2 | `PRODUCER_REGISTER` | 102 | `RESPONSE_PRODUCER_REGISTER` |
| 3 | `PCM` | 103 | `R_PCM` |
| 4 | `CONSUMER_REGISTER` | 104 | `RESPONSE_CONSUMER_REGISTER` |
| 5 | `COMMIT_OFFSET` | 107 | `COMMIT_OFFSET_ACK` |
| 6 | `ASSIGNMENT` | 105 | `ASSIGNMENT_ACK` |
| 7 | `FETCH` | 106 | `FETCH_ACK` |

### Payload quan trọng

| Message | Payload |
|---|---|
| `PRODUCER_REGISTER` | `port:uint16`, `topicID:uint16` |
| `CONSUMER_REGISTER` | `port:uint16`, `topicID:uint16`, `groupID:uint16` |
| `FETCH` | `partitionID:uint16`, `offset:uint32` |
| `FETCH_ACK` | `partitionID:uint16`, `found:bool`, `nextOffset:uint32`, encoded messages |
| `COMMIT_OFFSET` | `topicID:uint16`, `groupID:uint16`, `offset:uint32`, `partitionID:uint16` |
| `COMMIT_OFFSET_ACK` | `partitionID:uint16`, `offset:uint32`, `success:bool` |
| `ASSIGNMENT` | Số partition và danh sách `(partitionID:uint16, offset:uint32)` |

Protocol được định nghĩa và serialize/deserialize trong `message.go`.

## Cấu trúc source

| File | Nội dung |
|---|---|
| `kafka.go` | CLI entry point: `broker`, `producer`, `consumer`. |
| `broker.go` | TCP broker, đăng ký client, routing PCM, fetch, commit và assignment. |
| `producer.go` | TCP producer và vòng lặp đọc `stdin`. |
| `consumer.go` | TCP consumer, xử lý assignment/fetch/commit. |
| `topic.go` | Topic, round-robin partition selection và rebalance. |
| `partition.go` | Partition và queue tương ứng. |
| `cgroup.go` | Consumer group, membership và offset theo partition. |
| `queue.go` | Ring-buffer in-memory, fetch theo logical offset. |
| `message.go` | Message model, framing và binary encoding. |

## Storage và giới hạn

Queue hiện là ring buffer trong memory:

```go
ASLOT     = 255   // byte tối đa cho một slot
SLOT_SIZE = 10000 // số slot trong một queue
```

Mỗi partition có một `Queue` riêng với hai vùng array riêng: vùng data và vùng lưu độ dài message. Không còn dùng buffer global chung giữa các partition/topic.

Một topic được tạo với **3 partition cố định**. Offset được lưu trong memory, vì vậy broker restart sẽ mất toàn bộ message, topic registry, membership consumer group và committed offset.

## Concurrency

Project dùng goroutine để xử lý connection producer/consumer và `sync.RWMutex` cho queue. Queue copy message trước khi trả về để caller không giữ reference vào buffer nội bộ sau khi unlock.

Các state cần được đồng bộ khi tiếp tục phát triển gồm:

- registry `Broker.topics`;
- `Topic.nextPartition`, partition/group collection;
- membership, assignment và committed offset của `ConsumerGroup`;
- write trên cùng một consumer TCP stream.

Khi sửa concurrency, không copy struct đã chứa mutex. Ưu tiên lưu `*Topic`, `*ConsumerGroup`, `*Consumers` trong slice thay vì value struct. Kiểm tra bằng:

```bash
go vet ./...
go run -race . broker
```

## Giới hạn hiện tại

- Chỉ dành cho demo/local development; chưa có authentication, authorization hay TLS.
- Header frame chỉ có 1 byte length. Tổng `type + payload` phải không vượt `255` byte; batch `FETCH_ACK` lớn hoặc PCM dài có thể làm framing lỗi. Đây là giới hạn cần ưu tiên sửa bằng header length `uint32`.
- Không có persistence, replication, retention policy hoặc recovery sau restart.
- Partition count cố định là 3.
- Không có heartbeat, consumer leave handling, generation fencing hoặc cơ chế từ chối commit stale sau rebalance.
- Retry fetch rỗng hiện dùng `time.Sleep`, làm event loop consumer tạm dừng.
- Cơ chế concurrency vẫn đang được hoàn thiện; cần chạy workload thực tế với race detector sau mỗi thay đổi.

## Hướng phát triển đề xuất

1. Hoàn thiện ownership và mutex cho broker/topic/group; dùng pointer slices để tránh copy mutex.
2. Serialize write cho mỗi connection và thêm shutdown/disconnect handling.
3. Thay protocol length 1 byte bằng length `uint32`, thêm validate payload trước khi parse.
4. Thêm generation/member ID vào assignment và commit để chống stale commit sau rebalance.
5. Thêm persistence cho log và committed offset.
6. Viết unit test cho queue, protocol codec, rebalance và integration test producer/broker/consumer.

## License

Chưa có license được khai báo trong repository. Nếu muốn public hoặc tái sử dụng project, hãy thêm một file `LICENSE` phù hợp.
