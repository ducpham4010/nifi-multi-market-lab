# Phạm vi kiểm tra

Bộ lab được tạo và bắt đầu triển khai trên máy RHEL ngày 2026-11-10.
Hạ tầng đã được kiểm tra trực tiếp theo phạm vi dưới đây. Luồng Kafka → NiFi →
PostgreSQL staging đã chạy end-to-end ở bước 7.9; flow promotion và rollout thị trường còn chờ kiểm chứng.

## Kiểm tra trực tiếp trên máy lab

- Host RHEL 9.8 tại `192.168.102.4`, Docker Compose 5.5.1; đã pull các image được pin và build image NiFi kèm PostgreSQL JDBC driver.
- Kafka VN có `trips`, `trips-staging`; Kafka INDO có `trips`. Mỗi topic có 3 partition, replication factor 1 và có leader.
- Cả hai PostgreSQL đã có bảng `market.trip_events` và `staging.trip_events`; các bảng rỗng lúc seed ban đầu. Kết quả test staging bổ sung ở dưới.
- Cả hai Vault đã initialized/unsealed, seed KV riêng từng môi trường; file root token/unseal key nằm trong `.private/` với quyền `0600`.
- Cả ba NiFi đã đăng nhập bằng tài khoản admin, có context `shared-runtime` gồm 8 parameter và có JDBC JAR. Đã đối chiếu các giá trị không sensitive với Vault và kiểm tra `db.password` được đánh dấu Sensitive; API không trả lại giá trị password để đối chiếu.
- UI của cả ba NiFi trả HTTP 200 khi truy cập từ máy Mac qua LAN.
- Wrapper khởi động đặt cổng management riêng: staging `127.0.0.1:52020`, VN `127.0.0.1:52021`, INDO `127.0.0.1:52022`. Sau rebuild/recreate, số lần restart của cả ba container bằng 0 tại thời điểm kiểm tra.

- GitLab đã đăng nhập bằng root từ máy Mac qua LAN và gọi API xác nhận `username=root`, `is_admin=true`. Readiness qua loopback kiểm tra database, Redis, queues và Gitaly đạt `status=ok`.
- Đã hoàn tất production seed sau lỗi mật khẩu ban đầu: root active và có 9 work item types. Mật khẩu GitLab mặc định trong tài liệu đã đổi vì `adminadmin123` bị validation của image từ chối.
- Mount SSH GitLab dùng `/assets/sshd_config`; Nginx status mặc định cổng 8060 được tắt để tránh trùng với Doris trên host.
- Lượt kiểm tra cuối lúc khoảng 20:49 ngày 2026-11-10 (UTC+7), sau recreate GitLab với cấu hình cuối: `python3 scripts/check-infra.py` đạt toàn bộ kiểm tra, gồm 10 container, PostgreSQL qua LAN/password, các context NiFi còn nguyên, GitLab readiness và SSH banner trên cổng 18022.

Chạy lại trên máy lab:

```bash
cd ~/github/nifi-multi-market-lab
python3 scripts/check-infra.py
```

Thông tin truy cập và điểm bắt đầu bài thực hành ở [DEPLOYMENT_LAB.md](DEPLOYMENT_LAB.md).

## Bổ sung: phiên đăng nhập NiFi và guide UI

- Tái hiện trên lab: dùng cùng cookie jar đăng nhập staging qua IP trả 200; đăng nhập VN qua cùng IP rồi gọi lại staging trả 401. JWT cả hai đều có `exp - iat = 28800` giây, nên lỗi này là ghi đè cookie theo host/path.
- Đã cấu hình hostname riêng, allowlist proxy và certificate SAN cho staging/VN/INDO; vẫn dùng HTTPS và Single User authentication.
- `check-nifi-sessions.py` chạy từ máy Mac qua LAN: ba phiên cookie cùng tồn tại, đăng nhập lại staging không làm VN/INDO mất phiên; kiểm tra thời hạn token 8 giờ, không chờ thực tế 8 giờ để đo expiration.
- Snapshot API trước/sau rollout: giữ nguyên ID Process Group, processor state, connections và Parameter Context của ba instance. Tại thời điểm kiểm tra canvas chỉ có root, chưa có processor; kiểm tra này không chứng minh bảo toàn một flow đang xử lý dữ liệu.
- Test thực tế keytool/OpenSSL trong image NiFi: tạo PKCS12 lần đầu, chạy lại không đổi file, đổi hostname giữ password/trust anchor khác; `tests/test_browser_tls.py` đạt.
- Guide được mở rộng thành từng thao tác và đối chiếu source giao diện đúng tag `rel/nifi-2.6.0`, gồm menu Controller Services, Save Flow Version, Commit Local Changes, Import from Registry và Change Version. Browser runtime không có browser khả dụng; chưa chạy toàn bộ chuỗi click trên UI, chưa có screenshot xác nhận từng bước.

Chạy lại test TLS trong image:

```bash
docker compose exec -T -e NIFI_TLS_CONFIG_SCRIPT=/opt/nifi/scripts/configure-browser-tls.py \
  nifi-stag python3 - < tests/test_browser_tls.py
```

## Bổ sung: staging end-to-end ở bước 7.9

- Truy vết FlowFile `3d2a6e70-7cfb-4c82-b115-2ce6a05577ce`: ConsumeKafka RECEIVE → StampFlowVersion → PutDatabaseRecord SEND → LocalMarketTag → AuditSuccess DROP; Details cuối là `Auto-Terminated by success Relationship`.
- Khi DB vẫn rỗng, cấu hình live PutDatabaseRecord có **Statement Type = UPDATE**, Update Keys = event_id. Đây là UPDATE thành công nhưng không có row khớp; nhánh success/SEND/DROP không chứng minh row đã tồn tại.
- Đã backup cấu hình processor trong `.private/`, stop riêng PutDatabaseRecord, đổi duy nhất Statement Type thành **UPSERT**, xác nhận các property khác giữ nguyên rồi start lại.
- Gửi lại `bash scripts/produce.sh stag stag-v1-001`: query `staging.trip_events` trên PostgreSQL VN có đúng 1 row, `event_id=stag-v1-001`, `customer_id=C001`, `amount=125.50`. Bảng market của VN và INDO vẫn 0 row tại thời điểm kiểm tra.
- Test gửi qua Kafka và query DB thực tế; chưa publish version, tạo MR hoặc rollout VN/INDO trong lần kiểm tra này.

## Kiểm tra tĩnh và mock khi soạn bộ lab

- Docker Registry xác nhận HTTP 200 cho cả 5 image tag trong .env.example; đây là kiểm tra manifest trước khi triển khai trực tiếp.
- YAML parse thành công; đủ 10 services: 3 NiFi, 2 Kafka, 2 PostgreSQL, 2 Vault, 1 GitLab.
- Toàn bộ services dùng host network; không có Docker service hostname trong kết nối được hướng dẫn; các cổng chính được phân tách.
- Syntax của tất cả Python và Bash scripts.
- Snapshot guard với fixture sạch và fixture có parameter value, URL hard-code, LOCAL_ component, provider association: sạch pass; bốn loại vi phạm fail.
- HTTP mock cho init Vault lần đầu và chạy lại: seed không overwrite, không tạo token trùng; mock cho create context và async update NiFi; chỉ db.password mang Sensitive.
- Đối chiếu source NiFi tag 2.6.0 cho Default Branch, Parameter Context Values Remove, lịch sử version theo file path, ConsumeKafka properties và REST parameter-context update request.
- Đối chiếu Kafka Dockerfile cho thư mục data/volume và NiFi startup script cho HTTPS/Site-to-Site port variables.

Chạy lại kiểm tra HTTP mock:

```bash
python3 tests/test_scripts.py
```

## Phần chưa kiểm chứng

**Mock chỉ kiểm tra logic script, không chứng minh integration thật.** Chưa xác nhận
GitLab SSH key authentication/clone, import/change-version trên UI, round-trip Kafka → NiFi →
PostgreSQL tại runtime VN/INDO, rollout/rollback hay backup restore thực tế. Chưa kiểm tra chính sách
firewall/SELinux ở chế độ enforcing. Các bước thực hành có acceptance cases trong
CASES.md.

Trước sử dụng ngoài lab cần có test integration, đối chiếu phiên bản, image security updates, authentication/TLS và thiết kế HA riêng. Bản này dùng single-node để tập trung vào flow promotion.

## Bổ sung 2026-11-11: phát hiện khi triển khai bản Kubernetes

- Nhánh **cập nhật** context đã có của `scripts/sync-params.py` gửi update request thiếu `id` cấp entity; NiFi 2.6 trả `Failed to update Parameter Context ... The componentId must be specified`. Trên lab Compose chỉ nhánh tạo mới từng chạy thật nên lỗi không lộ ra. Đã sửa (thêm `id`) và thêm kiểm tra trong `tests/test_scripts.py`. Bản sửa đã chạy thật trên bản Kubernetes (cùng logic trong `k8s/nifi/base/nifi_k8s_sync.py`), chưa chạy lại trên stack Compose vì stack này đã dừng. Chi tiết: [VALIDATION_K8S.md](VALIDATION_K8S.md).
