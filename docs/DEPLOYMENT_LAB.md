# Hạ tầng sẵn có trên máy lab

Triển khai ngày 2026-11-10 trên RHEL 9.8, IP `192.168.102.4`.
Thư mục trên máy lab: `~/github/nifi-multi-market-lab`.
Compose project: `nifi-market-lab`.

## Truy cập

| Dịch vụ | URL | Tài khoản |
|---|---|---|
| NiFi staging | https://192.168.102.4:18443/nifi/ | `admin / adminadmin123` |
| NiFi VN | https://192.168.102.4:28443/nifi/ | `admin / adminadmin123` |
| NiFi INDO | https://192.168.102.4:38443/nifi/ | `admin / adminadmin123` |
| GitLab | http://192.168.102.4:18080 | `root / NifiLab_2026@Ready9` |
| Vault VN | http://192.168.102.4:18200/ui/ | Read token: `nifi-vn-lab-token` |
| Vault INDO | http://192.168.102.4:28200/ui/ | Read token: `nifi-indo-lab-token` |

NiFi dùng certificate tự ký; chấp nhận certificate của máy lab khi mở lần đầu.
Vault read token chỉ đọc/list KV; thao tác quản trị dùng root token trong
`.private/vault-vn-init.json` hoặc `.private/vault-indo-init.json` trên máy lab.

| Kết nối | VN | INDO |
|---|---|---|
| Kafka bootstrap | `192.168.102.4:19092` | `192.168.102.4:29092` |
| PostgreSQL | `192.168.102.4:15432` | `192.168.102.4:25432` |

PostgreSQL dùng database `market`, user/password `admin / admin`.
GitLab SSH dùng `192.168.102.4:18022` với SSH key của tài khoản GitLab.

## Đã chuẩn bị

- 3 NiFi, 2 Kafka, 2 PostgreSQL, 2 Vault persistent và GitLab CE.
- Kafka VN: `trips`, `trips-staging`; Kafka INDO: `trips`. Mỗi topic có 3 partition.
- Schema `market`, `staging` và bảng `trip_events` trên PostgreSQL; dữ liệu ban đầu rỗng.
- Vault VN có `kv/runtime`, `kv/staging`; Vault INDO có `kv/runtime`.
- Mỗi NiFi đã có context `shared-runtime`, đủ 8 parameter theo môi trường, password Sensitive và JDBC driver tại `/opt/nifi/drivers/postgresql.jar`.
- Profile `compose.lab.yaml` được bật qua `COMPOSE_FILE` trong `.env`: NiFi heap 256 MB–1 GB; GitLab Puma một process.
- Cổng management NiFi cố định trên loopback: staging 52020, VN 52021, INDO 52022.

## Bắt đầu bài thực hành

Bắt đầu tại **mục 4 của [GUIDE_VI.md](GUIDE_VI.md#4-tạo-gitlab-project-và-quyền)**:
tạo group/project `data360/flows-nifi`, branch, user và PAT. Sau đó kiểm tra context
đã có ở mục 5, cấu hình Registry Client ở mục 6 và tự dựng flow tại mục 7.

Các bước dựng flow, Save Version, MR, rollout và rollback là phần thực hành còn lại.
Xem [VALIDATION.md](VALIDATION.md) để biết phạm vi đã kiểm tra thực tế.

## Kiểm tra và khởi động lại

Chạy trên máy lab:

```bash
cd ~/github/nifi-multi-market-lab
docker compose ps
python3 scripts/check-infra.py
```

Các container dùng `restart: unless-stopped`. Sau khi Vault restart hoặc host reboot,
cần unseal lại bằng keys đã lưu:

```bash
docker compose up -d
python3 scripts/init-vault.py
python3 scripts/check-infra.py
```

Giữ nguyên `.env`, `.private/` và named volumes. `.private/` chứa keys/token;
không đưa vào Git. Khởi tạo Vault có thể chạy lại mà không ghi đè secret đã có.
