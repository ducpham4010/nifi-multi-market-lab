# NiFi multi-market flow lab — VN / INDO

Bộ lab thực hành Data 360: xây dựng flow Kafka → Postgres tại staging; lưu snapshot vào GitLab; review/MR sang main; người phụ trách từng thị trường import hoặc Change Version theo SHA đã duyệt; sử dụng parameter riêng từ Vault.

**Bắt đầu ở [docs/GUIDE_VI.md](docs/GUIDE_VI.md).** Tài liệu có đầy đủ thứ tự chạy, cấu hình UI, kiểm tra kết quả, phát hành lần hai, rollback và xử lý lỗi.

**Bản Kubernetes (branch `k8s-deployment`): [docs/GUIDE_K8S_VI.md](docs/GUIDE_K8S_VI.md).** Mỗi service một namespace, NiFi xác thực Vault bằng ServiceAccount + Kubernetes auth role (không còn token tĩnh), NiFi kết nối GitLab bằng Project Access Token, giao diện truy cập từ LAN qua Gateway API. Kết quả kiểm thử thực tế: [docs/VALIDATION_K8S.md](docs/VALIDATION_K8S.md).

| Bản K8s | URL (LAN) |
|---|---|
| NiFi staging / VN / INDO | `https://nifi-stag\|nifi-vn\|nifi-indo.192.168.102.4.sslip.io:31443/nifi/` |
| GitLab | `http://gitlab.192.168.102.4.sslip.io:31080` |
| Vault VN / INDO | `http://vault-vn\|vault-indo.192.168.102.4.sslip.io:31080/ui/` |

```bash
bash k8s/scripts/deploy.sh                 # image, Secret từ .env, kubectl apply -k k8s
python3 k8s/scripts/vault-init.py          # init/unseal, KV, Kubernetes auth + role
python3 k8s/scripts/gitlab-bootstrap.py    # project, branch, protect main, Project Access Token -> Vault
bash k8s/scripts/nifi-sync.sh params all   # Vault (ServiceAccount) -> shared-runtime
bash k8s/scripts/nifi-sync.sh registry all # PrAT -> Registry Client
python3 k8s/scripts/check-k8s.py           # kiểm tra sẵn sàng
```

Máy lab `192.168.102.4`: xem [thông tin truy cập và điểm bắt đầu thực hành](docs/DEPLOYMENT_LAB.md).

Truy cập nhanh trong workspace: [LAB_ACCESS.local.md](LAB_ACCESS.local.md) tổng hợp URL,
username, password và token thực tế. File này được giữ local và bỏ qua bởi Git.

Mở ba NiFi bằng hostname riêng trong file truy cập hoặc [mục 0 của guide](docs/GUIDE_VI.md#0-mở-đúng-ứng-dụng-và-chọn-điểm-bắt-đầu).
Ba tab cùng IP khác cổng sẽ ghi đè cookie đăng nhập của nhau; phiên Single User của NiFi 2.6 có thời hạn 8 giờ.

**Mới học NiFi?** Đọc [Hiểu NiFi từ những thành phần đầu tiên](docs/NIFI_CONCEPTS_VI.md) — giải thích các khái niệm, cách phối hợp và 7 sơ đồ minh họa. Có [bản PDF để đọc hoặc in](output/pdf/NIFI_CONCEPTS_VI.pdf).

GitLab/Vault do đội Ops quản lý: dùng [checklist yêu cầu và bàn giao Ops → Dev](docs/OPS_HANDOVER_VI.md).

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
| GitLab | root | NifiLab_2026@Ready9 |
| PostgreSQL | admin | admin |
| Vault đọc từ NiFi VN | Token | nifi-vn-lab-token |
| Vault đọc từ NiFi INDO | Token | nifi-indo-lab-token |

NiFi cần password tối thiểu 12 ký tự, nên không dùng `admin/admin`. Root token và unseal key Vault được sinh và lưu trong `.private/`, không tự đặt root token dễ nhớ trong chế độ persistent. Token dễ nhớ ở bảng chỉ có quyền đọc/list KV và thời hạn 30 ngày.

**Lab mở ra LAN với credential đơn giản và Kafka/DB/Vault/GitLab không TLS. Chỉ dùng mạng lab tin cậy, không public Internet.** NiFi dùng HTTPS với certificate tự ký.

## Files

| File | Chức năng |
|---|---|
| compose.yaml, .env.example | Toàn bộ hạ tầng và cấu hình host |
| nifi/Dockerfile, nifi/start-lab.sh | NiFi + JDBC driver, cổng management riêng cho host network |
| nifi/configure-browser-tls.py | Certificate tự ký có SAN cho hostname NiFi riêng; giữ store passwords và trust anchors khác |
| compose.lab.yaml | Profile giảm RAM cho máy lab dùng chung |
| scripts/preflight.sh | Kiểm tra IP/cổng/cấu hình |
| scripts/init-topics.sh | Tạo topics |
| scripts/init-vault.py | Init/unseal persistent Vault, seed secret nếu chưa có |
| scripts/check-infra.py | Kiểm tra readiness hạ tầng sau seed/sync parameter |
| scripts/check-nifi-sessions.py | Kiểm tra ba phiên cookie cùng tồn tại và thời hạn JWT |
| scripts/produce.sh, query.sh | Gửi dữ liệu và kiểm tra DB |
| scripts/release-sha.py | Liệt kê SHA của snapshot trên main |
| scripts/backup.sh | Backup offline tất cả named volumes và thông tin phục hồi |
| repo-template/ | Chỉ phần này có thể copy vào repo flows-nifi |
| docs/GUIDE_VI.md | Full walkthrough |
| docs/CASES.md | Kịch bản thực hành và tiêu chí đạt |
| docs/VALIDATION.md | Những gì đã kiểm tra và chưa chạy thực tế |
| k8s/ | Manifest Kustomize: namespaces, gateway, kafka, postgres, vault, gitlab, nifi (base + overlay stag/vn/indo) |
| k8s/nifi/base/nifi_k8s_sync.py | Job trong cụm: Vault (ServiceAccount) → `shared-runtime` và PrAT → Registry Client |
| k8s/scripts/ | deploy, vault-init, gitlab-bootstrap, nifi-sync, gitlab-token, produce/query, check-k8s |
| k8s/tests/e2e_lab.py | Kiểm thử end-to-end bản K8s qua API (staging → GitLab → VN/INDO, v2, rollback, Parameter Provider) |
| tests/test_k8s_manifests.py | Kiểm tra tĩnh manifest + logic sync (mock) |
| docs/GUIDE_K8S_VI.md, docs/VALIDATION_K8S.md | Hướng dẫn và kết quả kiểm thử bản Kubernetes |

Không commit cả thư mục triển khai vào repo snapshot; `.env`, `.private`, backup chứa cấu hình/secret local. Repo có sẵn chỉ cần bổ sung thư mục `flows/shared/.keep` và script kiểm tra nếu muốn.
