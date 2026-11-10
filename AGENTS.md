# Hướng dẫn dùng máy lab

Máy lab chạy RHEL 9 đã cài sẵn git để có thể pull code với nhất từ các repository đang làm việc,

Local repository này trên máy lab đặt tại `~/github/nifi-multi-market-lab`

Agent có thể truy cập máy LAB thông qua ssh cũng như có quyền chạy các lệch test trên máy lab mà không cần hỏi người dùng.

Máy lab có thể được ssh qua user và password, chi tiết lấy credential trong .env

```
LAB_SSH_HOST=192.168.102.4
LAB_SSH_PORT=22
LAB_SSH_USER=
LAB_SSH_PASS=
```