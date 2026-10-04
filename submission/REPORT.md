# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Đức Anh Quân / 202602405
**Repo:** https://github.com/QuannNguyen/K4-Track02-Day17-NguyenDucAnhQuan-202602405-DataPipelineEngineering.git
**Commit bài nộp:** fbf74549564cf474f0b59b1452db9dd4b07cd131
**AI đã dùng và phạm vi hỗ trợ:** GitHub Copilot; hỗ trợ kiểm tra logic, rà soát nguyên nhân, và đề xuất cách xác minh; mọi sửa code cuối cùng đều do tôi thực hiện và chạy trên môi trường local để kiểm chứng.
**Nguồn tham khảo khác (nếu có):** Không.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 ticket_id; `T-91` hiện nhiều phiên cũ và checksum Gold lệch. | `u05` trên 2026-08-12 chỉ đếm 2 sự kiện thay vì 5; `LOOKBACK_DAYS=0` không gom đủ dữ liệu trễ. | `T-97` còn xuất hiện trong snapshot và RAG; `is_deleted` vẫn là `False` với email/sđt còn lộ. |
| **Nguyên nhân gốc** | `upsert_silver_tickets()` dùng `INSERT` thay vì `MERGE` theo `ticket_id`, nên các batch cũ được append mà không kiểm tra LSN mới hơn. | `pipeline/config.py` đặt `LOOKBACK_DAYS = 0` trong khi Bronze đo được p99=3 ngày, và `gold_feature_daily` chỉ recompute theo ngày hiện tại. | Debezium delete có `after = null`; code lấy `ticket_id` từ `after` và không giữ tombstone, nên xóa thành “hàng sống” thay vì trạng thái `is_deleted = true`. |
| **Cách sửa** | `pipeline/silver.py`: đổi sang `MERGE INTO silver_tickets ... ON ticket_id` với điều kiện `s._lsn > t._lsn`; chỉ cập nhật khi LSN mới hơn. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3`; `build_feature_daily()` đã recompute `[day-3, day]` nên event trễ vẫn vào đúng event_date. | `pipeline/staging.py`: `ticket_id` dùng `coalesce(after, before)`; `ticket_changes_sql` giữ tombstone null cho user_id/subject/body; `silver_tickets` lưu `is_deleted=true`. |
| **Khái niệm trên slide** | Keyed upsert / MERGE by business key, never overwrite newer state | Microbatch + lookback = ceil(P99 lateness) | CDC tombstone + soft delete, not hard delete |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: vì đây là dữ liệu có khoá nghiệp vụ và cần idempotent khi re-run, không được “cộng dồn” các bản cũ.
- Tombstone thay vì xoá hẳn hàng trong Silver: vì CDC delete là sự kiện nghiệp vụ quan trọng; ta phải preserve “đã xoá” trên state mới nhất, đồng thời không làm mất lineage của Bronze.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: vì snapshot là phiên bản bất biến để kiểm soát train/serve và tái lập được lịch sử mô hình.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: vì dữ liệu ở mức lab không lớn, cần độ đơn giản, tốc độ build nhanh và dễ verify checksum parity.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào? Ở đây tôi tách rõ hai khái niệm: snapshot là state “as of” một thời điểm, còn quyền xoá nghĩa là state mới nhất không chứa ticket đó; vì vậy tôi lưu tombstone trong Silver để làm “đã xoá”, rồi khi build `gold_training_set` và `gold_doc_chunks` tôi lọc `l._op <> 'd'` và `NOT t.is_deleted`, chứ không ghi đè snapshot cũ. Snapshot cũ vẫn được giữ nguyên để audit và train lịch sử, còn snapshot mới nhất thì không còn ticket ấy.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao? Tôi đặt chốt PII ở tầng Bronze-to-Silver: các cột tự do văn bản `subject`, `body`, `text` được mask ngay khi vào Silver, và kiểm tra bằng regex trên tất cả output của Silver + Gold + thể hiện trong `scripts.verify` qua `PII_RE`; nếu tìm thấy vẫn có email/sđt thì fail. Đối với tên người, cần thêm danh sách hoặc NER/PII policy ở cùng tầng này; nếu dữ liệu đến từ text tự do, tôi chốt theo lớp “name entity” và đo bằng tỉ lệ làm rò rỉ trên tất cả text field sau khi mask, không chỉ regex email/phone.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
$ .\.venv\Scripts\python.exe -m pytest -q
..................................                                       [100%]
```

```text
$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ .\.venv\Scripts\python.exe main.py --land-only
$ cd dbt_project
$ ..\.venv\Scripts\dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
22:31:30  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.78 seconds (3.78s).
22:31:30  Completed successfully
22:31:30  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
