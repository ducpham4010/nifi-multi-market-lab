# Các bài thực hành và tiêu chí đạt

Mỗi release ghi: file path, SHA trên main, SHA trước/sau của VN/INDO, người rollout, giờ thực hiện, kết quả test, backup reference. Không ghi secret vào biên bản.

| Case | Thao tác | Kết quả phải thấy |
|---|---|---|
| 01: Staging độc lập | Produce stag-v1-001 | Chỉ schema staging trên PG VN có row |
| 02: Publish v1 | Save Version staging | Snapshot dưới flows/shared; có parameter names, không values |
| 03: Review chống leak | Chạy check_snapshot.py | PASS; manual diff không endpoint/credential/local PG |
| 04: Merge không deploy | Merge MR main, chưa import | Chưa có shared PG mới tại thị trường |
| 05: Import VN | Import M1, bind params VN | vn-v1-001 chỉ vào DB VN market |
| 06: Import INDO | Import cùng M1, params INDO | indo-v1-001 chỉ vào DB INDO |
| 07: Publish v2 | Marker flow.release=v2, Save/MR | Có M2 trong lịch sử snapshot main |
| 08: Rollout từng thị trường | VN Change Version M2; INDO giữ M1 | VN audit v2, INDO audit v1; merge không đồng bộ runtime |
| 09: Hội tụ | INDO Change Version M2 | Cùng SHA M2; params/logic local vẫn khác |
| 10: Local logic | Đổi local.market ở LOCAL_AUDIT VN | Shared PG không báo thay đổi do việc này; JSON không chứa local PG |
| 11: Rollback riêng | VN Change Version M1 | VN marker v1; INDO vẫn M2; DB/offset không rollback |
| 12: Revert qua Git | MR revert v2 → main | SHA M3, nội dung marker v1; rollout vẫn riêng |
| 13: Non-flow commit | Commit thay README | Branch HEAD đổi; version snapshot không bắt buộc đổi |
| 14: Replay | Produce cùng event_id hai lần | Một row theo PK; đây là UPSERT, không chứng minh exactly-once |
| 15: DB outage | Stop postgres-vn, produce VN | retry/failure giữ ở error path hoặc queue; không bị terminate |
| 16: Vault restart | Restart vault-vn | Vault sealed; init-vault.py unseal; secret vẫn còn |
| 17: NiFi restart | Restart nifi-vn | PG local/shared, context và version còn; kiểm tra queues/state |
| 18: Bad JSON | Produce JSON thiếu customer_id | Không ghi row; có FlowFile ở error path và lỗi có thể inspect |
| 19: New parameter | Thêm tham chiếu parameter ở staging | Chưa rollout nếu Vault/context market thiếu key; bổ sung contract/script trước |
| 20: Breaking port | Thử xóa/tạo lại port trong flow dùng riêng để test | Phát hiện broken connection/diff trước rollout; không dùng live PG cho thử nghiệm này |

## Bài lỗi DB và replay có kiểm soát

1. Dừng Postgres VN bằng `docker compose stop postgres-vn`.
2. Gửi `bash scripts/produce.sh vn vn-db-outage-001`.
3. Xem bulletin và queues. Trong một số lỗi driver/transaction, processor giữ FlowFile và retry thay vì đẩy ngay ra relationship; kiểm tra trạng thái thực tế.
4. Chạy `docker compose start postgres-vn`, chờ DB ready.
5. Nếu FlowFile còn ở input PutDatabaseRecord, processor xử lý khi DB trở lại. Nếu đã sang LOCAL_AUDIT/FAILED_RECORDS, không tự biến mất hoặc tự ghi DB.
6. Replay qua NiFi Provenance tại event đầu vào PutDatabaseRecord nếu content vẫn còn (menu Replay), hoặc gửi lại cùng event_id từ nguồn. Nếu chọn gửi lại, xác nhận DB có đúng row rồi mới xử lý record lỗi cũ để tránh replay hai đường ngoài ý muốn.
7. `query.sh vn` phải có một row. Kafka commit không phụ thuộc transaction Postgres, nên không coi consumer offset là bằng chứng DB đã ghi thành công.

## Mẫu biên bản

| Field | Value |
|---|---|
| Release label | v2 |
| Flow path | flows/shared/kafka-to-postgres.json |
| Approved main snapshot SHA | điền đầy đủ 40 ký tự |
| VN before / after | M1 / M2 |
| INDO before / after | M1 / M1 (chưa rollout) |
| VN parameter target | topic trips, schema market, VN endpoints |
| Probe event ID | vn-v2-001 |
| Audit evidence | flow.release=v2, local.market=VN |
| Local PG unaffected | yes/no + ghi nhận |
| Error queues | 0 hoặc giải thích |
| Rollback target | M1 |
| Operator / timestamp | điền khi thao tác |
