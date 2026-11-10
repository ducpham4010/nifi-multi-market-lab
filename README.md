# NiFi multi-market flow lab — VN / INDO

Bộ lab thực hành Data 360: xây dựng flow Kafka → Postgres tại staging; lưu snapshot vào GitLab; review/MR sang main; người phụ trách từng thị trường import hoặc Change Version theo SHA đã duyệt; sử dụng parameter riêng từ Vault.

**Bắt đầu ở [docs/GUIDE_VI.md](docs/GUIDE_VI.md).** Tài liệu có đầy đủ thứ tự chạy, cấu hình UI, kiểm tra kết quả, phát hành lần hai, rollback và xử lý lỗi.

## Phạm vi thiết kế

- 2 Kafka độc lập, mỗi Kafka 1 broker KRaft; 2 Postgres độc lập; 2 Vault persistent độc lập.
- 2 NiFi thị trường + **1 NiFi staging** riêng. Đây là phần bổ sung có chủ đích: nếu chỉ có 2 NiFi thì staging phải dùng chung runtime với một thị trường. Lab ưu tiên cách ly việc sửa/test. Staging dùng topic `trips-staging` và schema `staging` trên Kafka/Postgres VN, không thêm Kafka/Postgres thứ ba.
- Mỗi NiFi là **single-node instance**, chưa phải cluster HA nhiều node; mỗi Kafka cũng không kiểm chứng HA. Mục tiêu là flow promotion, parameter isolation, rollback và local customization.
- GitLab CE và một repo `data360/flows-nifi`; snapshot nằm dưới `flows/shared/`.
- Linux host networking, mọi kết nối giữa các dịch vụ dùng `${HOST_IP}:port`. Không dùng Docker service DNS.
- NiFi 2.6.0 được pin để cấu hình tái lập; Vault dùng KV v1. Image pin là baseline lab, không phải đề xuất phiên bản production mới nhất.
- Không có tự động deploy sau merge; cũng không tự commit mỗi thao tác trên canvas. **Save Version** mới gửi snapshot và tạo commit qua GitLab API.

## Mật khẩu lab

| Dịch vụ | User | Password/token |
|---|---|---|
| NiFi | admin | adminadmin123 |
| GitLab | root | adminadmin123 |
| PostgreSQL | admin | admin |
| Vault đọc từ NiFi VN | Token | nifi-vn-lab-token |
| Vault đọc từ NiFi INDO | Token | nifi-indo-lab-token |

NiFi cần password tối thiểu 12 ký tự, nên không dùng `admin/admin`. Root token và unseal key Vault được sinh và lưu trong `.private/`, không tự đặt root token dễ nhớ trong chế độ persistent. Token dễ nhớ ở bảng chỉ có quyền đọc/list KV và thời hạn 30 ngày.

**Lab mở ra LAN với credential đơn giản và Kafka/DB/Vault/GitLab không TLS. Chỉ dùng mạng lab tin cậy, không public Internet.** NiFi dùng HTTPS với certificate tự ký.

## Files

| File | Chức năng |
|---|---|
| compose.yaml, .env.example | Toàn bộ hạ tầng và cấu hình host |
| nifi/Dockerfile | NiFi + PostgreSQL JDBC driver |
| scripts/preflight.sh | Kiểm tra IP/cổng/cấu hình |
| scripts/init-topics.sh | Tạo topics |
| scripts/init-vault.py | Init/unseal persistent Vault, seed secret nếu chưa có |
| scripts/produce.sh, query.sh | Gửi dữ liệu và kiểm tra DB |
| scripts/release-sha.py | Liệt kê SHA của snapshot trên main |
| scripts/backup.sh | Backup offline tất cả named volumes và thông tin phục hồi |
| repo-template/ | Chỉ phần này có thể copy vào repo flows-nifi |
| docs/GUIDE_VI.md | Full walkthrough |
| docs/CASES.md | Kịch bản thực hành và tiêu chí đạt |
| docs/VALIDATION.md | Những gì đã kiểm tra và chưa chạy thực tế |

Không commit cả thư mục triển khai vào repo snapshot; `.env`, `.private`, backup chứa cấu hình/secret local. Repo có sẵn chỉ cần bổ sung thư mục `flows/shared/.keep` và script kiểm tra nếu muốn.
