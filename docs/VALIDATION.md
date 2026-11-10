# Phạm vi kiểm tra

Bộ lab được tạo ngày 2026-11-10. Đây là project để triển khai trên máy RHEL của người dùng; **chưa được chạy end-to-end trên RHEL/Docker** trong môi trường soạn thảo này vì không có Docker executable/daemon.

Đã kiểm tra:

- Docker Registry xác nhận HTTP 200 cho cả 5 image tag trong .env.example (chỉ xác nhận manifest tồn tại, chưa pull/run).
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

**Mock chỉ kiểm tra logic script, không chứng minh integration thật.** Chưa xác nhận image pull/build tại host người dùng, startup của toàn stack, import/change-version trên UI, round-trip Kafka → NiFi → Postgres, firewall/SELinux tại host hay backup restore thực tế. Các bước đó có acceptance cases trong CASES.md.

Trước sử dụng ngoài lab cần có test integration, đối chiếu phiên bản, image security updates, authentication/TLS và thiết kế HA riêng. Bản này dùng single-node để tập trung vào flow promotion.
