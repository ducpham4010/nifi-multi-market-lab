# Hạ tầng sẵn có trên máy lab

> **Từ 2026-11-11 lab chạy trên Kubernetes** (branch `k8s-deployment`), xem mục cuối tài liệu này và [GUIDE_K8S_VI.md](GUIDE_K8S_VI.md). Stack Docker Compose mô tả ngay dưới đây đã dừng; volume Docker và `.private/vault-*-init.json` của nó vẫn được giữ.

Triển khai ngày 2026-11-10 trên RHEL 9.8, IP `192.168.102.4`.
Thư mục trên máy lab: `~/github/nifi-multi-market-lab`.
Compose project: `nifi-market-lab`.

## Truy cập

| Dịch vụ | URL | Tài khoản |
|---|---|---|
| NiFi staging | https://nifi-stag.192.168.102.4.sslip.io:18443/nifi/ | `admin / adminadmin123` |
| NiFi VN | https://nifi-vn.192.168.102.4.sslip.io:28443/nifi/ | `admin / adminadmin123` |
| NiFi INDO | https://nifi-indo.192.168.102.4.sslip.io:38443/nifi/ | `admin / adminadmin123` |
| GitLab | http://192.168.102.4:18080 | `root / NifiLab_2026@Ready9` |
| Vault VN | http://192.168.102.4:18200/ui/ | Read token: `nifi-vn-lab-token` |
| Vault INDO | http://192.168.102.4:28200/ui/ | Read token: `nifi-indo-lab-token` |

NiFi dùng certificate tự ký; chấp nhận certificate của máy lab khi mở lần đầu.
Mỗi NiFi dùng hostname riêng để cookie không bị ghi đè khi đăng nhập nhiều tab.
Single User cấp JWT 8 giờ. Đóng tab IP cũ và dùng URL mới; mục 0 của guide có
dòng hosts dự phòng nếu DNS sslip.io bị chặn. Certificate SAN được chuẩn bị tự động
bởi wrapper khi container start, có cả hostname riêng và IP LAN.
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
python3 scripts/check-nifi-sessions.py
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

## Bản Kubernetes (đang chạy)

Triển khai ngày 2026-11-11 trên cụm kubeadm `lab-k8s` của cùng máy. Mỗi service một namespace; mọi object có nhãn `app.kubernetes.io/part-of=nifi-market-lab`.

| Dịch vụ | URL từ LAN | Đăng nhập |
|---|---|---|
| NiFi staging | https://nifi-stag.192.168.102.4.sslip.io:31443/nifi/ | `admin` / `NIFI_PASSWORD` |
| NiFi VN | https://nifi-vn.192.168.102.4.sslip.io:31443/nifi/ | như trên |
| NiFi INDO | https://nifi-indo.192.168.102.4.sslip.io:31443/nifi/ | như trên |
| GitLab | http://gitlab.192.168.102.4.sslip.io:31080 | `root` / `GITLAB_PASSWORD` |
| Vault VN / INDO | http://vault-vn.192.168.102.4.sslip.io:31080/ui/ · http://vault-indo.192.168.102.4.sslip.io:31080/ui/ | root token trong `.private/k8s/vault-<market>-init.json` |

- NiFi đọc Vault bằng ServiceAccount `nifi` của namespace mình (role `nifi-stag`, `nifi-vn`, `nifi-indo`); không còn token `nifi-*-lab-token`.
- NiFi kết nối GitLab bằng Project Access Token `nifi-staging-writer` / `nifi-vn-reader` / `nifi-indo-reader`, lưu trong Vault `kv/gitlab-registry/*`; không còn user `nifi-writer` / `nifi-reader` và PAT.
- Đã chạy e2e: flow của lần kiểm thử cuối đang chạy trên cả ba NiFi; project GitLab có MR !1 (v1), !2 (v2). Kết quả: [VALIDATION_K8S.md](VALIDATION_K8S.md).
- Hai container Docker `doris-fe`, `doris-be` đã được dừng để đủ RAM; bật lại: `docker start doris-fe doris-be`.

Kiểm tra / sau khi restart:

```bash
cd ~/github/nifi-multi-market-lab
python3 k8s/scripts/vault-init.py --unseal-only   # Vault luôn sealed sau khi pod restart
python3 k8s/scripts/check-k8s.py
```
