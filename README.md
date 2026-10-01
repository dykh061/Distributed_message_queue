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
- [Logging](#logging)
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

| Thành phần      | Vai trò                                                                         |
| --------------- | ------------------------------------------------------------------------------- |
| `Broker`        | Lắng nghe đăng ký tại port `10000`, quản lý topic, partition và consumer group. |
| `Topic`         | Có `topicID`, 3 partition cố định và danh sách consumer group.                  |
| `Partition`     | Có `partitionID` và một queue in-memory riêng.                                  |
| `Producer`      | Đọc từng dòng từ `stdin`, gửi `PCM` vào topic đã đăng ký.                       |
| `Consumer`      | Nhận assignment, fetch batch, xử lý message và commit offset.                   |
| `ConsumerGroup` | Lưu consumer tham gia group và committed offset theo partition.                 |

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

| Số consumer trong group | Assignment                                     |
| ----------------------: | ---------------------------------------------- |
|                       1 | Consumer 0: P0, P1, P2                         |
|                       2 | Consumer 0: P0, P2; Consumer 1: P1             |
|                       3 | Consumer 0: P0; Consumer 1: P1; Consumer 2: P2 |

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

| State                        | Ý nghĩa                                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------------------- |
| `assignment[].offset`        | Local position: offset mà consumer sẽ đọc/fetch tiếp.                                           |
| `pendingCommit[partitionID]` | Offset đã gửi commit nhưng chưa nhận ACK.                                                       |
| `commitedOffset[]`           | Offset broker đã xác nhận commit. Tên field hiện tại giữ nguyên theo source (`commitedOffset`). |

`COMMIT_OFFSET_ACK` mang `partitionID`, `offset` và `success`, giúp consumer ghép ACK với đúng commit đang chờ. Với một consumer quản lý nhiều partition, pending commit được lưu theo `partitionID`.

## TCP protocol

Mọi frame có format:

```text
+-------------+------+----------------+
| length: 4   | type | payload         |
+-------------+------+----------------+
```

- `length`: 4 byte big-endian (`uint32`), là kích thước của `type + payload`.
- `type`: 1 byte, xác định message type.
- `payload`: nhị phân, phụ thuộc vào type.
- Encoding số nguyên trong header và payload là big-endian.

### Message types

| Code | Request             | Code | Response / ACK               |
| ---: | ------------------- | ---: | ---------------------------- |
|    1 | `ECHO`              |  101 | `RESPONSE_ECHO`              |
|    2 | `PRODUCER_REGISTER` |  102 | `RESPONSE_PRODUCER_REGISTER` |
|    3 | `PCM`               |  103 | `R_PCM`                      |
|    4 | `CONSUMER_REGISTER` |  104 | `RESPONSE_CONSUMER_REGISTER` |
|    5 | `COMMIT_OFFSET`     |  107 | `COMMIT_OFFSET_ACK`          |
|    6 | `ASSIGNMENT`        |  105 | `ASSIGNMENT_ACK`             |
|    7 | `FETCH`             |  106 | `FETCH_ACK`                  |

### Payload quan trọng

| Message             | Payload                                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `PRODUCER_REGISTER` | `port:uint16`, `topicID:uint16`                                                                                           |
| `CONSUMER_REGISTER` | `port:uint16`, `topicID:uint16`, `groupID:uint16`                                                                         |
| `FETCH`             | `partitionID:uint16`, `offset:uint32`, `generation:uint32`                                                                |
| `FETCH_ACK`         | `partitionID:uint16`, `found:bool`, `nextOffset:uint32`, `generation:uint32`, các message dạng `[length:uint16][payload]` |
| `COMMIT_OFFSET`     | `topicID:uint16`, `groupID:uint16`, `offset:uint32`, `partitionID:uint16`, `generation:uint32`                            |
| `COMMIT_OFFSET_ACK` | `partitionID:uint16`, `offset:uint32`, `generation:uint32`, `success:bool`                                                |
| `ASSIGNMENT`        | Số partition, `generation:uint32` và danh sách `(partitionID:uint16, offset:uint32)`                                      |

Protocol được định nghĩa và serialize/deserialize trong `message.go`.

## Logging

Log mặc định tập trung vào các sự kiện cần theo dõi trong terminal:

- Broker khởi động và producer/consumer kết nối hoặc mất kết nối.
- Message được route vào partition và message mới được consumer xử lý.
- Consumer gửi commit và nhận commit ACK.
- Các lỗi fetch, commit, generation và consumer bị loại khỏi group.

Các hoạt động polling bình thường như fetch liên tục, partition rỗng, assignment ACK và queue debug không được in ra để tránh làm trôi log message.

## Cấu trúc source

| File           | Nội dung                                                              |
| -------------- | --------------------------------------------------------------------- |
| `kafka.go`     | CLI entry point: `broker`, `producer`, `consumer`.                    |
| `broker.go`    | TCP broker, đăng ký client, routing PCM, fetch, commit và assignment. |
| `producer.go`  | TCP producer và vòng lặp đọc `stdin`.                                 |
| `consumer.go`  | TCP consumer, xử lý assignment/fetch/commit.                          |
| `topic.go`     | Topic, round-robin partition selection và rebalance.                  |
| `partition.go` | Partition và queue tương ứng.                                         |
| `cgroup.go`    | Consumer group, membership và offset theo partition.                  |
| `queue.go`     | Ring-buffer in-memory, fetch theo logical offset.                     |
| `message.go`   | Message model, framing và binary encoding.                            |

## Storage và giới hạn

Queue hiện là ring buffer trong memory:

```go
ASLOT     = 255   // byte tối đa cho một slot
SLOT_SIZE = 10000 // số slot trong một queue
```

Mỗi partition có một `Queue` riêng với hai vùng array riêng: vùng data và vùng lưu độ dài message. Độ dài message trong queue được lưu bằng một `byte`, vì vậy payload tối đa là `255` byte. Không còn dùng buffer global chung giữa các partition/topic.

Queue hiện có `10.000` slot nhưng chưa tự động từ chối khi đầy. Vì broker chưa gọi `pop` để giải phóng slot, không nên gửi quá sức chứa này trên một partition; nếu vượt quá, dữ liệu cũ có thể bị ghi đè và offset không còn đáng tin cậy.

Một topic được tạo với **3 partition cố định**. Offset được lưu trong memory, vì vậy broker restart sẽ mất toàn bộ message, topic registry, membership consumer group và committed offset.

## Concurrency

Project dùng goroutine để xử lý connection producer/consumer và `sync.RWMutex` cho queue. Queue copy message trước khi trả về để caller không giữ reference vào buffer nội bộ sau khi unlock.

Các state cần được đồng bộ khi tiếp tục phát triển gồm:

- registry `Broker.topics`;
- `Topic.nextPartition`, partition/group collection;
- membership, assignment và committed offset của `ConsumerGroup`;
- write trên cùng một consumer TCP stream, vì assignment và response có thể cùng được gửi khi rebalance.

Khi sửa concurrency, không copy struct đã chứa mutex. Ưu tiên lưu `*Topic`, `*ConsumerGroup`, `*Consumers` trong slice thay vì value struct. Kiểm tra bằng:

```bash
go vet ./...
go run -race . broker
```

## Giới hạn hiện tại

- Chỉ dành cho demo/local development; chưa có authentication, authorization hay TLS.
- Queue chỉ lưu message tối đa `255` byte và chưa xử lý đầy queue một cách an toàn.
- Không có persistence, replication, retention policy hoặc recovery sau restart.
- Partition count cố định là 3.
- Không có heartbeat hoặc session timeout; broker phát hiện consumer mất kết nối chủ yếu khi thao tác đọc connection lỗi.
- Broker có kiểm tra generation để từ chối fetch và commit stale sau rebalance, nhưng commit chưa kiểm tra đầy đủ partition được assign và giới hạn offset hợp lệ.
- Retry fetch rỗng hiện dùng `time.Sleep`, làm event loop consumer tạm dừng.
- Assignment và response chưa có cơ chế serialize write riêng cho từng connection.
- Commit, assignment và message chỉ được lưu trong memory.

## Hướng phát triển đề xuất

1. Thêm kiểm tra đầy queue và validate payload tối đa `255` byte trước khi push.
2. Serialize write cho mỗi connection và thêm shutdown/disconnect handling.
3. Validate consumer assignment, partition tồn tại và offset hợp lệ trước khi nhận commit.
4. Thêm heartbeat/session timeout cho consumer group.
5. Thêm persistence cho log và committed offset.
6. Viết unit test cho queue overflow, protocol codec, rebalance, commit validation và integration test producer/broker/consumer.

## License

Chưa có license được khai báo trong repository. Nếu muốn public hoặc tái sử dụng project, hãy thêm một file `LICENSE` phù hợp.
