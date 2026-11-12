# Kết quả kiểm thử bản Kubernetes

Triển khai và kiểm thử ngày **2026-11-11** (UTC+7) trên máy lab `192.168.102.4`, branch `k8s-deployment`. Hướng dẫn: [GUIDE_K8S_VI.md](GUIDE_K8S_VI.md).

## Môi trường

| Thành phần | Phiên bản / cấu hình |
|---|---|
| Host | RHEL 9.8, 8 vCPU, 21 GiB RAM, không swap |
| Kubernetes | kubeadm v1.37.1, 1 node `lab-k8s`, containerd 2.3.6, flannel |
| Storage | `local-path` (rancher local-path-provisioner), reclaim Delete |
| Ingress | Envoy Gateway v1.9.3, GatewayClass `eg-lab`; Gateway riêng `lab-gateway/lab` NodePort 31080/31443 |
| Image | `apache/kafka:3.9.1`, `postgres:16.9`, `hashicorp/vault:1.19.5`, `gitlab/gitlab-ce:18.2.1-ce.0`, `localhost:31102/nifi-market-lab/nifi:2.6.0-lab1` (build từ `nifi/Dockerfile`, NiFi 2.6.0) |

Để đủ RAM cho GitLab, đã dừng hai container Docker không thuộc lab `doris-fe`, `doris-be` (`docker stop`, không xóa; bật lại bằng `docker start doris-fe doris-be`). Doris trên Kubernetes (namespace `doris`) và các ứng dụng khác giữ nguyên. Stack Docker Compose cũ đã dừng từ trước; volume `nifi-market-lab_*` và `.private/vault-*-init.json` không bị đụng tới.

Sau kiểm thử, 10 pod của lab dùng khoảng 6.3 GiB RAM thực tế theo `kubectl top` (GitLab 2.5 GiB, mỗi NiFi ~0.95 GiB, mỗi Kafka ~0.4 GiB); host còn ~6.9 GiB khả dụng.

## 1. Triển khai và hạ tầng

- `deploy.sh`: build/push image NiFi vào registry trong cụm, nạp image từ Docker vào containerd, tạo 11 namespace và Secret từ `.env`, apply Kustomize; tất cả StatefulSet Ready, Job tạo topic Complete. GitLab Ready sau lần khởi động đầu.
- `vault-init.py`: init + unseal 2 Vault, seed `kv/runtime`, `kv/staging`, `kv/preview` (VN) và `kv/runtime` (INDO) với endpoint DNS Service, bật `auth/kubernetes`, tạo 3 policy + 3 role. Key lưu `.private/k8s/vault-*-init.json` (0600).
- `gitlab-bootstrap.py`: tạo group/project/branch/protect main/merge settings và 3 Project Access Token, ghi vào Vault. Chạy lại lần hai: giữ token (`keep ... still active`), không tạo trùng.
- `nifi-sync.sh params all` và `registry all`: Job ở mỗi namespace NiFi đăng nhập Vault bằng `ServiceAccount nifi` (`role=nifi-stag|nifi-vn|nifi-indo`, policy đúng instance), tạo `shared-runtime` và Registry Client; client đọc được branch và bucket `shared`.

**`check-k8s.py` (lần cuối, sau e2e): 57 PASS, 0 FAIL, `All checks passed`.** Bao gồm:

- 10 StatefulSet Ready, không OOMKilled; Gateway Programmed.
- Topic `trips`, `trips-staging` (VN), `trips` (INDO): 3 partition có leader. Bảng `market`/`staging.trip_events` trên cả hai PostgreSQL.
- Vault initialized/unsealed, key file 0600 ngoài cụm.
- Vault Kubernetes auth: 3 role login đúng ServiceAccount và đọc được secret của mình; **bị từ chối**: đọc secret instance khác (`nifi-stag`→`kv/runtime`, `nifi-vn`→`kv/staging`, `nifi-indo`→`kv/staging`), SA `nifi-indo/nifi` dùng role `nifi-vn`, SA `nifi-vn/nifi` dùng role `nifi-stag`, SA `nifi-vn/default`, token thiếu audience `vault`, Vault INDO với role `nifi-vn`.
- 3 NiFi: UI qua Gateway, đúng một `shared-runtime`, 8 parameter bằng giá trị Vault, chỉ `db.password` Sensitive; Registry Client đọc branch đúng.
- GitLab readiness `all=1` ok, UI qua Gateway, SSH banner NodePort 31022; 3 PrAT active, đúng scope, user là bot.

## 2. Truy cập từ LAN

Từ máy Mac trong LAN (không phải host lab):

| Kiểm tra | Kết quả |
|---|---|
| `https://nifi-stag|vn|indo.192.168.102.4.sslip.io:31443/nifi/` | HTTP 200 cả ba |
| `http://gitlab.192.168.102.4.sslip.io:31080/users/sign_in` | 200 |
| `http://vault-vn|vault-indo.192.168.102.4.sslip.io:31080/ui/` | 200 |
| Certificate NiFi VN qua TLS passthrough | `CN=nifi-vn.192.168.102.4.sslip.io`, SAN: hostname trình duyệt, `localhost`, `nifi.nifi-vn.svc`, `nifi.nifi-vn.svc.cluster.local`, `nifi-0.nifi-headless.nifi-vn.svc.cluster.local`, IP `192.168.102.4` |
| `NIFI_GATEWAY_PORT=31443 python3 scripts/check-nifi-sessions.py` | 3 phiên cookie cùng tồn tại, JWT 8 giờ, cookie domain độc lập kể cả sau khi đăng nhập lại staging |

## 3. End-to-end (`k8s/tests/e2e_lab.py`)

Lần chạy cuối `run=1111162450`, 16:24:54–16:25:51 (57 giây), **exit 0, `E2E PASSED`**, trên đúng code của commit. Bằng chứng: `.private/k8s/e2e-evidence-1111162450.json` và `.private/k8s/e2e-final.log` trên máy lab. Ba lần chạy trước đó dừng vì lỗi đã sửa ở mục 6.

| # | Bước | Kết quả |
|---|---|---|
| 1 | Staging dựng `MARKET_RUNTIME/{kafka-to-postgres, LOCAL_AUDIT}` theo GUIDE_VI mục 7 | Service Enabled, component valid, flow running |
| 2 | `stag-k8s-v1-1111162450` | 1 row ở `postgres-vn/staging`, không có ở `postgres-vn/market`, `postgres-indo/market`; provenance AuditSuccess DROP: `flow.release=v1`, `local.market=STAG` |
| 3 | Save Version v1 qua `gitlab-staging` | S1 `0fb3d34175c04c733b69fe3d880faab524b2d99f`, commit author `nifi-staging-writer` (bot của PrAT) |
| 4 | `check_snapshot.py` | PASS; không có LOCAL_AUDIT, DNS `svc.cluster.local` hay password trong property |
| 5 | PrAT reader ghi file lên `staging` | Bị từ chối |
| 6 | MR !1 staging → main do `nifi-staging-writer` tạo | Writer merge bị từ chối (protected main); merge bởi `lab-release-maintainer`; M1 = `0fb3d341…` |
| 7 | VN, INDO import M1 từ `main` qua `gitlab-main` (PrAT reader) | PG bind `shared-runtime` local (KEEP_EXISTING), LOCAL_AUDIT riêng |
| 8 | `vn-k8s-v1-…`, `indo-k8s-v1-…` | Mỗi event chỉ vào DB thị trường của nó; audit `v1/VN`, `v1/INDO` |
| 9 | v2 trên staging (Commit Local Changes) | S2 `ed6df5cc21684c4212926ac378eb5ab1c17f0096`; `stag-k8s-v2-…` audit `v2/STAG`; snapshot guard PASS |
| 10 | MR !2 → main | M2 = `ed6df5cc…`, writer merge bị từ chối |
| 11 | VN Change Version M1 → M2 | `vn-k8s-v2-…` audit `v2/VN`; context và LOCAL_AUDIT giữ nguyên |
| 12 | INDO sau khi merge M2 | Vẫn M1, `indo-k8s-still-v1-…` audit `v1/INDO` (merge không tự rollout) |
| 13 | VN rollback M2 → M1 | `vn-k8s-rollback-v1-…` audit `v1/VN`; INDO không đổi |
| 14 | Job `vault-param-sync` của VN khi flow đang chạy | `Updated shared-runtime` bằng ServiceAccount `nifi-vn/nifi`; `vn-k8s-after-resync-…` vẫn vào DB, audit `v1/VN` |
| 15 | NiFi VN `HashiCorpVaultParameterProvider`, client `Vault Authentication=KUBERNETES`, role `nifi-vn`, token `/var/run/secrets/vault/token` | Fetch `kv/preview` → `vault-preview` 8 parameter, `kafka.group=nifi-vn-preview`, `db.password` Sensitive, có provider association |

Token Maintainer `lab-release-maintainer` (chỉ để merge tự động) đã bị thu hồi sau lần chạy (`gitlab-bootstrap.py --revoke-release-token`).

Trạng thái để lại sau kiểm thử: ba NiFi đang chạy flow của lần e2e cuối (staging v2, VN M1 sau rollback, INDO M1), project GitLab có MR !1, !2 đã merge, NiFi VN có `vault-preview`. Muốn tự thực hành từ canvas rỗng: mục 3.7 của GUIDE_K8S_VI (`--reset-only`, `--recreate-project`).

## 4. Restart và bền vững dữ liệu

Rolling restart toàn bộ 10 StatefulSet (đổi nhãn pod template) trước khi chạy e2e:

- Vault sealed sau restart (`sealed before: True`), `vault-init.py --unseal-only` unseal được, secret và role còn nguyên.
- NiFi giữ `shared-runtime`, Registry Client (Access Token nằm trong flow đã mã hóa bằng sensitive props key) nhờ PVC; `check-k8s.py` đạt toàn bộ ngay sau restart.
- GitLab giữ project và token; Kafka giữ topic.

Lệnh trong tài liệu đã chạy thử: `git clone` qua Gateway với username `nifi-staging-writer` + PrAT làm password (clone được, `check_snapshot.py` PASS trên `main`); `release-sha.py` với `GITLAB_URL` K8s và PrAT reader liệt kê commit `v2: release marker`, `v1: kafka to postgres`, `Registering Flow [kafka-to-postgres]` (hai lệnh này chạy trên project của lần e2e trước; project hiện tại đã được tạo lại bởi lần chạy cuối); login Vault bằng SA `nifi-indo/nifi` vào role `nifi-vn` trả `{"errors":["namespace not authorized"]}`.

## 5. Kiểm tra tĩnh và đơn vị

| Lệnh | Kết quả |
|---|---|
| `python3 tests/test_k8s_manifests.py` | 12 test OK: mỗi StatefulSet một namespace riêng (10/10), không có Secret/token tĩnh trong `k8s/`, NiFi dùng SA `nifi` + token projected audience `vault`, không automount, CronJob suspended và đúng role/path/branch, TLSRoute/HTTPRoute/NodePort/ClientTrafficPolicy, `init.sql` trùng bản Compose, tag image khớp; logic sync (mock): login bằng JWT SA, không dùng token tĩnh, contract 8 key, chỉ `db.password` Sensitive, `revoke-self`, nhánh update gửi `id` cấp entity, resolve property Registry Client; snapshot guard chặn DNS Service K8s |
| `tests/test_browser_tls.py` chạy trong pod NiFi | 2 test OK, gồm SAN bổ sung từ `NIFI_TLS_EXTRA_SANS` và tái sử dụng certificate khi restart |
| `python3 tests/test_scripts.py` | PASS; thêm kiểm tra `id` cấp entity cho update request của `scripts/sync-params.py` (Compose) |

## 6. Lỗi phát hiện và đã sửa trong quá trình kiểm thử

| Lỗi | Nguyên nhân | Sửa |
|---|---|---|
| API GitLab qua Gateway: 302 rồi 404 với `projects/data360%2Fflows-nifi` | Envoy Gateway mặc định `escapedSlashesAction: UnescapeAndRedirect` | `ClientTrafficPolicy lab-keep-encoded-paths` (`KeepUnchanged`) |
| `Groups::CreateService`: "Organization can't be blank" | GitLab 17+ đặt group trong organization | Truyền `organization_id` của organization `default` |
| `gitlab-rails runner` không đọc được script | Runner chạy bằng user `git`, file 0600 của root | Ghi script tạm với quyền 0644 (không chứa secret) |
| `kubectl apply` bị Forbidden khi thêm nhãn | `includeTemplates` của Kustomize gắn nhãn cả `volumeClaimTemplates` (bất biến) | Gắn nhãn trực tiếp vào pod template |
| Job tạo topic không apply lại được | Pod template của Job bất biến | `ttlSecondsAfterFinished: 300`, apply tạo lại Job (idempotent) |
| Job `vault-param-sync` lỗi khi `shared-runtime` **đã tồn tại**: `The componentId must be specified` | NiFi 2.6 `ParameterUpdateManager` tạo revision từ `id` cấp entity; payload chỉ có `component.id`. Lỗi có sẵn ở `scripts/sync-params.py` của bản Compose (nhánh update chưa từng chạy thật trên lab cũ) | Thêm `id` cấp entity ở cả hai script; test mock cho nhánh update; e2e bước 14 |
| `nifi-sync.sh` chờ đủ 5 phút khi Job đã Failed; Job tự chạy lại một lần | `kubectl wait --for=condition=complete` không dừng khi Failed; `backoffLimit: 1` | Vòng lặp dừng ở trạng thái cuối; `backoffLimit: 0` |

## 7. Chưa kiểm chứng / giới hạn

- Chuỗi click trên UI chưa được ghi lại từng bước bằng trình duyệt; e2e dùng cùng REST API mà UI gọi. Đã kiểm tra UI trả 200 và đăng nhập được từ LAN.
- Single-node: không HA cho NiFi, Kafka, Vault, GitLab; không kiểm tra node failure hay PodDisruptionBudget.
- Flannel không thực thi NetworkPolicy, nên cô lập mạng giữa namespace chưa được áp dụng; cô lập hiện ở mức namespace, RBAC/ServiceAccount và Vault policy.
- GitLab/Vault HTTP, Kafka PLAINTEXT, certificate NiFi tự ký; Job sync bỏ kiểm tra certificate NiFi (có thể bật bằng `NIFI_CA_FILE`).
- Backup/restore PVC theo mục 12.3–12.4 GUIDE_K8S_VI chưa chạy thử phục hồi thực tế.
- `down.sh` / `up.sh` (thêm 2026-11-12) chưa chạy gỡ và dựng lại thật, vì lab đang được giữ nguyên trạng thái theo yêu cầu. Đã kiểm tra trên lab: cú pháp; `down.sh` liệt kê đúng 11 namespace và 11 PVC, rồi từ chối khi không có terminal và không có `--yes`; `kubectl delete -k k8s --dry-run=server` chỉ chạm object của lab (11 namespace, 2 ClusterRoleBinding, 10 StatefulSet, 20 Service, 13 ConfigMap, 6 CronJob, 5 ServiceAccount, Gateway/route/policy); hàm chờ PV nhận đúng 11 trên tổng 21 PV của cụm. Các bước bên trong `up.sh` đã chạy riêng lẻ trong lần triển khai ở trên.
- Token ServiceAccount projected hết hạn sau 1 giờ và được kubelet xoay; chưa chạy dài hạn để quan sát Parameter Provider re-login sau `token_max_ttl` (4 giờ).
