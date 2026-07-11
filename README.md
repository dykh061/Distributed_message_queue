# DistributedMQ

DistributedMQ là một demo message queue đơn giản bằng Go, gồm hai tiến trình chính:

- `broker`: lắng nghe tại cổng `10000`
- `producer`: mở một cổng riêng, đăng ký với broker bằng `port` và `topicID`, sau đó gửi message dạng `PCM`

## Cách chạy

### 1. Chạy broker

```bash
go run . broker
```

### 2. Chạy producer

Producer cần 2 tham số:

- `port`: cổng local mà producer sẽ lắng nghe
- `topicID`: topic mà producer muốn đăng ký

```bash
go run . producer 10001 18
```

## Luồng hoạt động

1. Broker khởi động và listen trên cổng `10000`.
2. Producer khởi động và listen trên cổng của nó, ví dụ `10001`.
3. Producer dial tới broker và gửi message `PRODUCER_REGISTER`.
4. Payload đăng ký chứa 2 trường:
   - `port` của producer
   - `topicID`
5. Broker đọc message, parse theo type `PRODUCER_REGISTER`, rồi chuyển 4 byte payload về struct `ProducerRegister` bằng `fromByte`.
6. Broker kiểm tra danh sách topic:
   - nếu `topicID` đã tồn tại thì lấy `idx` của topic đó
   - nếu chưa có thì tạo topic mới và gán `idx`
7. Broker dial ngược lại producer tại `port` mà producer đã gửi lên.
8. Sau đó broker và producer giao tiếp qua kết nối này bằng message type `PCM`.
9. Khi producer gửi `PCM`, broker gọi `processProducerPCM` để đẩy message vào đúng queue của topic tương ứng, rồi trả ACK về producer.
10. Trong khi xử lý một producer, broker vẫn tiếp tục vòng lặp accept để nhận producer mới.

## Protocol message

Project dùng chung một lớp message để định nghĩa:

- hằng số type message
- struct dữ liệu của từng loại message
- hàm đọc/ghi stream
- hàm chuyển đổi giữa struct và byte

### Format gói tin

Mỗi message được viết theo thứ tự:

```text
[độ dài][type][payload]
```

- `độ dài`: 1 byte
- `type`: 1 byte
- `payload`: phần dữ liệu theo từng loại message

### `PRODUCER_REGISTER`

Payload của `PRODUCER_REGISTER` gồm 4 byte:

- 2 byte cho `port`
- 2 byte cho `topicID`

Hàm `toByte()` chuyển struct `ProducerRegister` sang 4 byte.
Hàm `fromByte()` chuyển 4 byte về struct để broker xử lý.

## Các type message chính

- `ECHO`
- `PRODUCER_REGISTER`
- `PCM`
- `RESPONSE_ECHO`
- `RESPONSE_PRODUCER_REGISTER`
- `R_PCM`

## Thành phần chính

- `kafka.go`: điểm vào chương trình, chọn chạy broker hoặc producer
- `broker.go`: xử lý đăng ký producer, quản lý topic và queue
- `producer.go`: mở server local, đăng ký với broker, gửi PCM
- `message.go`: định nghĩa protocol và hàm đọc/ghi message
- `topic.go`: struct topic
- `queue.go`: queue lưu PCM theo topic

## Ghi chú

- Producer hiện gửi PCM khi người dùng nhập một dòng từ stdin.
- Broker chỉ cần topicID lúc producer đăng ký; sau đó PCM được đẩy vào topic đã map sẵn.
- Đây là demo TCP đơn giản, chưa có consumer và chưa có cơ chế đồng bộ hóa cho nhiều producer cùng lúc.