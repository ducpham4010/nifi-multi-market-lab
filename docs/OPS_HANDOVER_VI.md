# Yêu cầu Ops thiết lập GitLab và Vault cho NiFi

Tài liệu dùng làm nội dung ticket và biên bản bàn giao **Ops → Dev**. Áp dụng cho flow Kafka → PostgreSQL, ba môi trường NiFi staging / VN / INDO, baseline NiFi 2.6.0 của repo này.

Các ô `<...>` do hai đội điền khi triển khai. Không điền token/password thật vào bản Markdown lưu Git; chỉ ghi đường dẫn secret hoặc mã tham chiếu bàn giao qua kênh bảo mật.

## 1. Nội dung ticket gửi Ops

> Đề nghị cấp project GitLab để quản lý phiên bản flow NiFi và vùng secret Vault cho staging, VN, INDO. Staging được ghi snapshot vào branch staging; NiFi VN/INDO chỉ đọc phiên bản đã duyệt trên main. Vault cung cấp cấu hình Kafka/PostgreSQL theo từng môi trường. Đề nghị Ops hoàn thành các mục 3–5, bàn giao bảng mục 7 và phối hợp kiểm tra mục 8. Dev cấu hình NiFi theo thông tin đã bàn giao; không yêu cầu quyền quản trị GitLab/Vault.

| Thông tin yêu cầu | Điền trước khi gửi |
|---|---|
| Dự án / mã ticket | `<project / ticket>` |
| Dev owner và nhóm người dùng | `<tên / nhóm SSO>` |
| Ops owner / kênh hỗ trợ | `<nhóm / kênh / giờ hỗ trợ>` |
| Ngày cần sẵn sàng | `<ngày>` |
| NiFi staging / VN / INDO | `<URL UI và địa chỉ nguồn egress của từng instance>` |
| Máy chạy bridge / công cụ release | `<host hoặc runner và địa chỉ nguồn egress>` |
| Người/nhóm được duyệt và merge release | `<nhóm release approver>` |
| Cách lấy secret | `<nhập tay / bridge / native Parameter Provider>` |
| Cách xác thực workload | `<Token theo lab / phương án doanh nghiệp đã kiểm chứng>` |
| Phạm vi | `<thực hành / dev-test / production; yêu cầu availability>` |

Nếu doanh nghiệp đã có GitLab/Vault, yêu cầu cấp tài nguyên và quyền trên hệ thống hiện có; không cần dựng thêm server chỉ để khớp tên service Docker trong lab.

## 2. Phân công rõ trách nhiệm

| Công việc | Ops | Dev / người phụ trách release |
|---|---|---|
| GitLab/Vault server, DNS, TLS, backup, monitoring, phục hồi | Chủ trì | Cung cấp yêu cầu, phối hợp nghiệm thu |
| GitLab group/project, tài khoản workload, token, branch protection | Thiết lập và quản lý vòng đời | Cung cấp danh sách người dùng và mục đích truy cập |
| Review nội dung flow và duyệt release | Cấp quyền cho nhóm đã thống nhất | Reviewer duyệt; người có quyền thực hiện merge |
| Vault mount, policy, authentication, cấp và thu hồi credential | Chủ trì | Cung cấp mapping môi trường và danh sách key |
| Giá trị endpoint, topic, DB user/password | Phối hợp đội Kafka/DB xác nhận rồi nạp Vault | Xác nhận hợp đồng dữ liệu và yêu cầu sử dụng |
| NiFi Registry Client, Parameter Context, Controller Services, flow | Bàn giao kết nối/quyền cần thiết | Cấu hình và kiểm thử |
| Đồng bộ giá trị sau khi Vault thay đổi | Thông báo thay đổi và phối hợp rotation | Fetch/Apply hoặc chạy bridge, kiểm thử flow |
| Rollout NiFi sau merge GitLab | Hỗ trợ khi có lỗi nền tảng | Chọn SHA đã duyệt và rollout từng instance |

Không mặc định Ops là người review nghiệp vụ flow. Cũng không mặc định merge GitLab sẽ tự deploy NiFi.

## 3. GitLab: Ops cần setup

### 3.1 Project và cấu trúc repository

- [ ] Cấp GitLab qua HTTPS, DNS và chuỗi CA mà NiFi có thể tin cậy.
- [ ] Tạo project **Private**, tên đề xuất `data360/flows-nifi`; bàn giao namespace thực tế nếu khác.
- [ ] Initialize repository, có branch `main` và đặt `main` làm default branch.
- [ ] Tạo `flows/shared/.keep` trên main rồi tạo branch `staging` từ main.
- [ ] Xác nhận `flows` là Repository Path, `shared` là bucket. Snapshot dự kiến: `flows/shared/kafka-to-postgres.json`.
- [ ] Cấp tài khoản SSO cho Dev để xem repository, tạo MR và review theo phân công.

Project này chứa snapshot flow. Nó khác repository mã nguồn `nifi-multi-market-lab` chứa Compose, script và tài liệu.

### 3.2 Danh tính và quyền của NiFi

Tách danh tính workload khỏi tài khoản cá nhân người vận hành. Đề xuất mỗi instance một credential để thu hồi/rotate độc lập.

| Danh tính đề xuất | Dùng ở đâu | Role trong project | Scope token | Quyền mong muốn |
|---|---|---|---|---|
| `nifi-staging-writer` | Registry Client staging | Developer | `api` | Đọc repo, ghi snapshot vào staging; không push main |
| `nifi-vn-reader` | Registry Client VN | Reporter | `read_api` | Đọc branch/commit/snapshot; không ghi repo |
| `nifi-indo-reader` | Registry Client INDO | Reporter | `read_api` | Đọc branch/commit/snapshot; không ghi repo |
| Nhóm review/release | GitLab UI | Role đủ review/merge theo phân công | Không dùng token workload để duyệt MR | Merge main theo quy trình |

Ops có thể cấp **project access token** nếu phiên bản, license và chính sách cho phép (đã kiểm chứng trên GitLab CE 18.2.1 self-managed trong bản K8s: NiFi 2.6 GitLabFlowRegistryClient đọc/ghi bình thường, commit mang tên bot của token); hoặc PAT của tài khoản kỹ thuật chỉ có membership cần thiết. Không yêu cầu PAT của admin. Role và scope cùng quyết định quyền token; scope `api` không tự giới hạn token chỉ được ghi một branch. Branch protection thực hiện phần giới hạn đó. [GitLab: project access tokens](https://docs.gitlab.com/user/project/settings/project_access_tokens/).

Registry Client sử dụng REST API: `read_repository` đơn thuần không thay cho `read_api`. Password đăng nhập web và SSH deploy key cũng không thay cho trường Access Token của cấu hình hiện tại. [GitLab: token scopes](https://docs.gitlab.com/security/tokens/access_token_scopes/).

### 3.3 Bảo vệ release

- [ ] Bảo vệ `main`: quyền merge dành cho Maintainer hoặc nhóm release được chỉ định theo tính năng hệ thống.
- [ ] Chặn push trực tiếp vào main đối với workload và Dev thông thường; tắt force push.
- [ ] Cho phép writer ghi staging; kiểm tra cả rule wildcard và quyền kế thừa từ group.
- [ ] Giữ branch staging sau merge để NiFi tiếp tục publish phiên bản sau.
- [ ] Để tái hiện đúng lab: Merge commit, không squash. Nếu doanh nghiệp dùng quy tắc khác, thống nhất lại cách lấy SHA release và thử trọn chu kỳ publish/merge/rollout.
- [ ] Ghi rõ reviewer, người merge và bằng chứng cần có trước khi duyệt. Nếu GitLab không hỗ trợ enforcement mong muốn, ghi rõ bước kiểm soát thủ công.

Không cần GitLab Runner cho việc Registry Client lưu/đọc snapshot. CI kiểm tra snapshot là hạng mục riêng nếu có yêu cầu.

### 3.4 Nếu PAT GitLab được lưu trong Vault

Ops có thể lưu PAT trong vùng riêng, ví dụ `<mount>/nifi-registry/staging`, `/vn`, `/indo`, key `access_token`. Không thêm PAT vào secret runtime 8 key ở mục 4.2.

NiFi 2.6 Registry Client không bind Parameter Context. Vì vậy Fetch PAT vào context rồi nhập `#{gitlab.token}` không cấu hình được client. Chọn một cách bàn giao:

1. Người vận hành được ủy quyền lấy token qua kênh bảo mật, nhập vào Access Token của Registry Client; hoặc
2. Ops/automation đọc Vault rồi gọi NiFi API để cấu hình client; Dev nhận trạng thái đã cấu hình và metadata token, không nhất thiết nhận giá trị token.

Cách 2 cần quyền NiFi riêng và automation được triển khai; repo hiện chưa có bridge Vault → Registry Client. Khi PAT rotate, phải cập nhật client, không chỉ đổi secret trong Vault. [Source NiFi 2.6: Registry Client không có Parameter Context](https://github.com/apache/nifi/blob/rel/nifi-2.6.0/nifi-framework-bundle/nifi-framework/nifi-framework-components/src/main/java/org/apache/nifi/registry/flow/StandardFlowRegistryClientNode.java#L120).

## 4. Vault: Ops cần setup

### 4.1 Endpoint, mount và phân tách môi trường

- [ ] Cấp URL API/UI qua HTTPS, CA chain, và namespace nếu hệ thống sử dụng namespace.
- [ ] Xác nhận Vault hoạt động, đã unseal; Ops chịu trách nhiệm restart/unseal hoặc auto-unseal, backup/restore và audit logging.
- [ ] Cấp mount và ghi rõ **KV v1 hay KV v2**. Không suy luận KV version từ phiên bản Vault server.
- [ ] Tách quyền đọc staging, VN, INDO; không cấp một token đọc mọi secret của doanh nghiệp.
- [ ] Bàn giao mapping endpoint + namespace + mount + secret path của từng môi trường.

Mapping tương thích lab hiện tại:

| Môi trường NiFi | Vault logic | Mount | KV version | Secret path trong mount |
|---|---|---|---|---|
| staging | Vault VN | `kv` | 1 | `staging` |
| VN | Vault VN | `kv` | 1 | `runtime` |
| INDO | Vault INDO | `kv` | 1 | `runtime` |
| Bài thử provider, tùy chọn | Vault VN | `kv` | 1 | `preview` |

Nếu chỉ có một Vault dùng chung, chọn path phân biệt như `nifi/staging/runtime`, `nifi/vn/runtime`, `nifi/indo/runtime`. Không dùng cùng một path `kv/runtime` cho cả VN và INDO trên cùng endpoint/namespace. Mapping mới cần cập nhật cấu hình/script trước khi dùng.

### 4.2 Nội dung secret runtime

Mỗi secret chứa **đúng 8 key dưới đây, tất cả là string**, để tương thích kiểm tra hợp đồng của bridge hiện tại. Ops lấy giá trị thật từ đội sở hữu Kafka/DB; Dev xác nhận topic, schema và bảng mong muốn.

| Key | staging | VN | INDO | Sensitive trong NiFi |
|---|---|---|---|---|
| `kafka.bootstrap` | `<broker staging>` | `<broker VN>` | `<broker INDO>` | No |
| `kafka.topic` | `trips-staging` | `trips` | `trips` | No |
| `kafka.group` | `nifi-staging` | `nifi-vn` | `nifi-indo` | No |
| `db.url` | `<JDBC URL staging>` | `<JDBC URL VN>` | `<JDBC URL INDO>` | No |
| `db.username` | `<DB user staging>` | `<DB user VN>` | `<DB user INDO>` | No |
| `db.password` | `<secret reference>` | `<secret reference>` | `<secret reference>` | **Yes** |
| `db.schema` | `staging` | `market` | `market` | No |
| `db.table` | `trip_events` | `trip_events` | `trip_events` | No |

`<secret reference>` trong bảng chỉ là placeholder của biên bản. **Giá trị `db.password` lưu trong Vault phải là password thật**, vì bridge hiện đọc trực tiếp, không tự giải tham chiếu tới secret khác.

Lưu password vào Vault không tạo DB user hay đổi password PostgreSQL. Cần xác nhận user đã tồn tại và đủ quyền kết nối, sử dụng schema, đọc/ghi theo UPSERT của lab. Kafka cần topic và quyền đọc/consumer group tương ứng. Nếu Kafka yêu cầu SASL/TLS hoặc DB có yêu cầu xác thực khác, hợp đồng 8 key chưa đủ: bàn giao thêm phương thức xác thực, CA và secret cần thiết, rồi Dev sửa cấu hình flow/bridge trước nghiệm thu.

### 4.3 Authentication và policy

| Chủ thể | Quyền cần có | Không cấp mặc định |
|---|---|---|
| Bridge staging | Read đúng secret staging | Update/delete secret, quản trị mount/policy |
| Bridge VN / INDO | Read secret của đúng môi trường | Read môi trường khác |
| Native Parameter Provider | Read secret được chọn; List ở prefix cần khám phá | Read toàn bộ mount chỉ để Fetch chạy được |
| Người Dev được phép nhập tay | Login bằng cơ chế doanh nghiệp; read đúng secret nếu được phê duyệt | Root token/unseal key |
| Người quản lý secret của Ops | Ghi/rotate theo quy trình nội bộ | Chia sẻ danh tính quản trị cho ứng dụng |

Ví dụ policy tối thiểu cho **bridge staging, KV v1**, với mapping lab:

```hcl
path "kv/staging" {
  capabilities = ["read"]
}
```

Native provider cần list tùy cấu trúc đường dẫn nó duyệt. Ops bổ sung list ở prefix cần thiết và thử Fetch với đúng danh tính workload. `Secret Name Pattern` là bộ lọc của provider, không phải ranh giới phân quyền. List có thể lộ tên secret dù không cho đọc giá trị; nếu cần giấu tên giữa các nhóm, tách prefix/mount phù hợp. Quyền UI để duyệt mount cũng phải kiểm tra riêng, không mở rộng read toàn mount để sửa lỗi UI. [Vault: policies](https://developer.hashicorp.com/vault/docs/concepts/policies).

KV v2 dùng đường API/policy `data/` và `metadata/` khác KV v1. Ops phải cung cấp policy đúng engine; không copy nguyên ví dụ KV v1 sang KV v2. [HashiCorp: quyền theo đường dẫn KV](https://support.hashicorp.com/hc/en-us/articles/6572815701907-How-to-give-access-for-specific-path-inside-KV-secret).

Bàn giao auth method, auth mount, role và cách lấy credential ban đầu. Lab dùng Token; nếu chuyển AppRole hoặc cơ chế khác, phải kiểm tra hỗ trợ và cấu hình trên component NiFi 2.6 đang dùng. RoleID/SecretID không thể được dán thay Vault token vào bridge hiện tại.

**Khi NiFi chạy trên Kubernetes** (bản `k8s/`, đã kiểm chứng trong lab): dùng Vault **Kubernetes auth**, không cấp token tĩnh. Ops bàn giao auth mount (`kubernetes`), tên role theo instance và audience; Dev/Platform bảo đảm mỗi NiFi chạy bằng ServiceAccount riêng trong namespace riêng và mount token projected đúng audience.

| Hạng mục Ops cấu hình | Giá trị trong lab |
|---|---|
| Auth mount / `kubernetes_host` | `kubernetes` / `https://kubernetes.default.svc.cluster.local` (Vault trong cụm dùng token SA của chính nó để gọi TokenReview, cần ClusterRoleBinding `system:auth-delegator`) |
| Role staging / VN / INDO | `nifi-stag` / `nifi-vn` (Vault VN), `nifi-indo` (Vault INDO) |
| `bound_service_account_names` / `_namespaces` | `nifi` / đúng một namespace: `nifi-stag`, `nifi-vn`, `nifi-indo` |
| `audience` | `vault` (pod mount `serviceAccountToken` projected với audience này) |
| TTL | `token_ttl=1h`, `token_max_ttl=4h` |
| Policy | Chỉ `read` secret của instance + `list` tên trên `kv/`; staging không đọc `kv/runtime` và ngược lại |

NiFi 2.6 dùng được cơ chế này ở hai nơi: Job đồng bộ trong namespace NiFi, và `StandardHashiCorpVaultClientService` với **Vault Authentication = KUBERNETES**, dynamic property `vault.kubernetes.role` và `vault.kubernetes.service-account-token-file`. Chi tiết: [GUIDE_K8S_VI.md](GUIDE_K8S_VI.md) mục 5.

### 4.4 Vòng đời secret và token

- [ ] Ghi thời hạn, TTL/max TTL và renewable hay không; không mặc định mọi token đều renew được.
- [ ] Chỉ định ai renew/rotate, lịch cảnh báo trước hết hạn và cách xử lý khi bị revoke.
- [ ] Khi đổi DB password: phối hợp thay đổi ở DB, Vault và NiFi theo cửa sổ đã thống nhất; kiểm thử kết nối và dữ liệu sau đổi.
- [ ] Khi đổi Vault token: cập nhật bridge hoặc Vault Client Service tương ứng rồi thử đọc/Fetch.
- [ ] Khi đổi GitLab PAT: cập nhật Registry Client và thử đọc/publish theo quyền.
- [ ] Ghi quy trình xử lý sự cố và rollback nếu credential mới chưa hoạt động.

Fetch/Apply hoặc bridge là thao tác đồng bộ có chủ đích. Chỉ cập nhật Vault không làm Parameter Context của NiFi tự cập nhật trong thiết kế lab này.

## 5. Mạng và TLS cần bàn giao

| Nguồn | Đích | Mục đích |
|---|---|---|
| NiFi staging/VN/INDO | GitLab HTTPS/API | Registry Client đọc/ghi snapshot theo quyền |
| NiFi dùng native provider | Vault HTTPS/API | Xác thực, list/read parameter |
| Máy chạy bridge | Vault API và NiFi API đích | Đọc secret và cập nhật context |
| Máy chạy công cụ release | GitLab API | Tra SHA main |
| Máy Dev được phép | GitLab/Vault/NiFi UI | Review, cấu hình và kiểm tra |
| NiFi runtime | Kafka và PostgreSQL | Xử lý bản tin; phối hợp đội sở hữu dịch vụ |

Ops bàn giao FQDN, port, DNS, proxy/no-proxy nếu có và CA chain. Đội quản lý NiFi cấu hình truststore/SSL Context Service theo component cần dùng; test từ chính host/container gọi API. Browser mở được UI chưa chứng minh NiFi kết nối được API. SSH GitLab không bắt buộc cho Registry Client dùng REST.

## 6. Điểm phải đổi khi chuyển từ lab sang dịch vụ Ops quản lý

| Thành phần hiện tại | Giới hạn đã thấy trong repo | Việc cần thống nhất |
|---|---|---|
| `.env.example` | Chủ yếu cấu hình Compose tự dựng dịch vụ, chưa có bộ biến endpoint/auth riêng đầy đủ | Lập cấu hình tích hợp theo thông tin Ops bàn giao; không coi sửa HOST_IP là đủ |
| `scripts/sync-params.py` | Hard-code port, HTTP Vault, mount KV1, path và token lab; bỏ kiểm tra cert NiFi | Sửa để nhận endpoint/auth/CA/path phù hợp, xác minh TLS trước khi dùng ngoài lab |
| `scripts/release-sha.py` | URL GitLab dựng từ HOST_IP và port HTTP lab | Sửa base URL/CA/proxy theo GitLab thực tế |
| `scripts/init-vault.py` | Khởi tạo/unseal/seed Vault lab và policy rộng trong mount lab | Không chạy lên Vault doanh nghiệp; Ops cấp mount/policy/secret theo ticket |
| GUIDE mục 4 và phần tạo secret | Hướng dẫn người học tự quản trị bằng tài khoản lab | Ops thực hiện phần nền tảng; Dev dùng bảng bàn giao để làm mục 5–6 |
| GUIDE mục 13 | Provider preview dùng KV1, Token và path lab | Chọn engine/auth/path thật; thử preview riêng trước runtime |

Tài liệu này là yêu cầu triển khai và bàn giao; không tự sửa các script trên. Nếu Ops chuẩn hóa KV2 hoặc auth khác, không cần hạ chuẩn của Ops để khớp lab: ghi chênh lệch và hoàn tất phần tích hợp trước nghiệm thu.

## 7. Biểu mẫu Ops bàn giao cho Dev

### 7.1 GitLab — thông tin không bí mật

| Trường | Ops điền |
|---|---|
| URL UI / API base URL | `<https://gitlab.../>`; Registry Client dùng base URL, không thêm `/api/v4` |
| API version | `V4` hoặc xác nhận khác |
| Namespace / repository name / project ID | `<...> / <...> / <...>` |
| Repository Path / bucket | `flows` / `shared` hoặc mapping thực tế |
| Branch publish / branch release | `staging` / `main` |
| NiFi client staging / VN / INDO | `gitlab-staging` / `gitlab-main` / `gitlab-main` hoặc tên đã thống nhất |
| Writer: loại token, danh tính, role, scope, hạn dùng | `<...>` |
| VN reader: loại token, danh tính, role, scope, hạn dùng | `<...>` |
| INDO reader: loại token, danh tính, role, scope, hạn dùng | `<...>` |
| Nơi nhận secret từng token | `<Vault path / mã bàn giao bảo mật; không ghi token>` |
| Reviewer / người được merge main | `<...>` |
| Branch rules / merge strategy / giữ staging | `<link cấu hình hoặc bằng chứng đã kiểm tra>` |
| CA / proxy / allowlist đã mở | `<...>` |
| Owner rotation / ngày nhắc hết hạn | `<...>` |

Dev cấu hình **Parameter Context Values = Remove** cho Registry Client theo thiết kế lab; cấu hình này không tự xóa literal secret bị hard-code trong flow. Review snapshot trước merge.

### 7.2 Vault — điền riêng từng môi trường

| Trường | staging | VN | INDO |
|---|---|---|---|
| API URL / UI URL | `<...>` | `<...>` | `<...>` |
| Namespace hoặc xác nhận không dùng | `<...>` | `<...>` | `<...>` |
| Mount / KV version | `<...>` | `<...>` | `<...>` |
| Secret path tương đối trong mount | `<...>` | `<...>` | `<...>` |
| Auth method / auth mount / role | `<...>` | `<...>` | `<...>` |
| Danh tính và policy name | `<...>` | `<...>` | `<...>` |
| Quyền read/list đã kiểm tra | `<...>` | `<...>` | `<...>` |
| Cách nhận credential / tham chiếu bảo mật | `<...>` | `<...>` | `<...>` |
| TTL / max TTL / renewable | `<...>` | `<...>` | `<...>` |
| Ai renew/rotate / lịch cảnh báo | `<...>` | `<...>` | `<...>` |
| Xác nhận đủ 8 key, DB/Kafka credentials có hiệu lực | `<...>` | `<...>` | `<...>` |
| CA / proxy / nguồn mạng được phép | `<...>` | `<...>` | `<...>` |

Nếu làm bài provider preview, thêm mapping và quyền cho secret preview riêng; không cho Dev sửa runtime chỉ để thử Fetch.

### 7.3 Dev thực sự nhận gì?

1. URL, namespace/project/path, branch và CA để cấu hình kết nối.
2. Tài khoản người dùng theo SSO và quyền đúng dự án.
3. Credential workload có phạm vi/hạn dùng rõ ràng **hoặc** xác nhận Ops đã cấu hình credential vào NiFi qua automation.
4. Mapping secret theo môi trường và hợp đồng 8 key đã xác nhận.
5. Bằng chứng nghiệm thu, đầu mối hỗ trợ và quy trình đổi/thu hồi credential.

Dev không cần nhận GitLab root password, Vault root token, unseal/recovery keys hay quyền admin nền tảng. Nếu chính sách không cho Dev đọc secret production, Ops hoặc automation thực hiện cấu hình nhạy cảm; Dev nhận metadata và bằng chứng test thay cho secret.

## 8. Kiểm tra trước khi ký nhận

Thực hiện từ đúng nguồn mạng và bằng đúng danh tính workload; không dùng admin/root để chứng minh token ứng dụng chạy được. Các thử nghiệm ghi dùng branch/path thử được hai đội thống nhất, không thử sửa snapshot release đang dùng.

| Kiểm tra | Kết quả cần có | Bằng chứng bàn giao |
|---|---|---|
| DNS/TLS/API từ NiFi và máy bridge | Kết nối được, xác minh certificate thành công | Thời gian, nguồn, endpoint, kết quả đã che secret |
| Writer đọc repo và lưu flow thử trên staging | Có commit và snapshot đúng bucket | Commit SHA / đường dẫn |
| Writer bị chặn push main | Policy và kiểm thử có kiểm soát xác nhận chặn | Rule hiệu lực / kết quả test |
| Reader VN/INDO đọc snapshot main | Import hoặc truy xuất đúng SHA thành công | Instance, branch, SHA |
| Reader không ghi được | Role/scope và thử nghiệm có kiểm soát xác nhận chặn | Kết quả không chứa credential |
| Vault đọc đúng secret | Đủ 8 key/string, không in password vào biên bản | Tên key, environment, thời gian |
| Vault chặn secret môi trường khác | Danh tính riêng không đọc được ngoài phạm vi | Path thử và kết quả từ chối |
| Native provider nếu dùng | Fetch/List/Apply context preview thành công | Provider/context, số key, db.password Sensitive |
| NiFi nối Kafka/DB bằng cấu hình đã bàn giao | Produce bản tin thử, query đúng bảng có row | Event ID, schema/bảng, kết quả đối chiếu |
| Review/MR/release | Merge đúng quyền, giữ staging, market chọn SHA đã duyệt | MR URL + SHA + instance rollout |
| Rotation mẫu ở môi trường thử | Credential mới hoạt động; cũ bị thu hồi theo kế hoạch | Người thực hiện, thời gian, kết quả |

Không chỉ dùng trạng thái Enabled, HTTP UI 200 hay AuditSuccess để ký nhận toàn bộ tích hợp. Chốt cả quyền truy cập, phiên bản flow và dữ liệu đích.

| Ký nhận | Thông tin |
|---|---|
| Ops xác nhận | `<người / ngày / ticket>` |
| Dev xác nhận | `<người / ngày>` |
| Hạng mục chưa đạt / người xử lý / hạn hoàn tất | `<...>` |
| Link runbook vận hành và escalation | `<...>` |

## 9. Tài liệu liên quan

- [GUIDE_VI.md](GUIDE_VI.md): mục 4 GitLab, mục 5 parameters, mục 6 Registry Client, mục 13 native Vault provider.
- [GUIDE_K8S_VI.md](GUIDE_K8S_VI.md): bản Kubernetes — Vault Kubernetes auth theo ServiceAccount, Project Access Token lưu trong Vault, Job cấu hình Registry Client (cách 2 của mục 3.4).
- [NIFI_CONCEPTS_VI.md](NIFI_CONCEPTS_VI.md): giải thích vai trò các thành phần.
- [GitLab protected branches](https://docs.gitlab.com/user/project/repository/branches/protected/): quyền push/merge và các rule bảo vệ branch.
- [Vault KV v1](https://developer.hashicorp.com/vault/docs/secrets/kv/kv-v1) và [KV v2](https://developer.hashicorp.com/vault/docs/secrets/kv/kv-v2): khác biệt engine để Ops và Dev thống nhất trước triển khai.
