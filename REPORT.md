# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Tuấn Khanh` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602819` |
| GitHub | `Ataraxiza` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | <điền: trip-xxxxx> |

## Kết quả cuối

- dataset_version của bạn: <điền> (phải khớp report/evidence/run_summary.json)
- Số khung hình train / val / test của bạn: <điền>
- report/verify.txt: tạo bằng python3 scripts/check_submission.py --save-verify (phải có 27/27 checks passed).
- Ảnh chụp trong report/screenshots/: airflow_graph.png, minio_lake.png, metabase_dashboard.png, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

# Trả lời
Khác biệt giữa telemetry nguồn A và B ở bronze:
- Số dòng: A có 3021 dòng, B có 3234 dòng.
- Timestamp: A dùng ts_utc dạng ISO 8601, B dùng timestamp_ms dạng Unix milliseconds.
- Tốc độ: A dùng speed_mps (m/s), B dùng spd_kmh (km/h).
- Hướng di chuyển: A dùng heading_deg, B dùng hdg.
- Tọa độ: A dùng lat, lon; B dùng latitude, longitude.
- Gia tốc: A dùng acc_x, acc_y; B dùng ax, ay.
- Chân ga và phanh: A dùng gas, brake; B dùng throttle_pct, brake_pct.

Lý do giữ mỗi nguồn một bảng bronze và lưu cột dạng chữ:
- Giữ nguyên dữ liệu gốc, tránh mất thông tin do đổi tên cột hoặc chuyển đổi dữ liệu quá sớm.
- Tránh lỗi ép kiểu khi hai nguồn có định dạng timestamp, đơn vị và schema khác nhau.
- Dễ truy vết nguồn dữ liệu, kiểm tra lỗi và đối chiếu với file gốc.
- Đảm bảo ingest có tính idempotent: chạy lại không tạo thêm dữ liệu trùng.
Việc chuẩn hóa kiểu dữ liệu, tên cột và đơn vị sẽ được thực hiện ở lớp Silver; Gold dùng dữ liệu đã chuẩn hóa để phân tích.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

# Trả lời
Pipeline chuẩn hóa hai nguồn telemetry:
- Schema: Chuẩn hóa tên cột của A và B về cùng một schema.
- Đơn vị: Chuyển tốc độ của B từ km/h sang m/s.
- Kiểu thời gian: Chuyển timestamp của A và B về cùng kiểu thời gian.
- Độ lệch đồng hồ: Trừ độ lệch 1,5 giây của drive B theo cấu hình trong sources.json.
- Bằng chứng: Drive B bắt đầu tại 2025-10-08 14:30:00.009+00:00, khớp với thời điểm bắt đầu video 2025-10-08 14:30:00, sai lệch chỉ 0,009 giây. Tốc độ tối đa của A là 71 km/h, B là 100 km/h, đều hợp lý. Kiểm tra tự động PASS 7/7.

Bảng quarantine
drive_id	reject_reason	so_dong
drive_a	duplicate	20
drive_a	speed_missing	30
drive_a	speedoutof_range	3
drive_b	duplicate	20
drive_b	speed_missing	30
drive_b	speedoutof_range	3
Tổng cộng: 106 dòng bị cách ly, gồm 53 dòng mỗi drive.

Lý do đưa dòng lỗi vào silver.silver_telemetry_rejects thay vì xóa:
- Bảo toàn dữ liệu để kiểm tra và truy vết nguyên nhân lỗi.
- Tránh mất dữ liệu gốc và hỗ trợ xử lý lại khi cần.
- Đảm bảo chất lượng bảng telemetry sạch, không chứa dòng trùng, thiếu tốc độ hoặc tốc độ bất hợp lý.
- Giúp kiểm chứng rằng số dòng sạch cộng số dòng bị cách ly bằng tổng số dòng đầu vào.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

# Trả lời

Lý do vì sao max_gap_ms là warn còn in_range là error:
- max_gap_ms là warn vì khoảng trống tín hiệu cảm biến cần được cảnh báo nhưng không nhất thiết phải dừng pipeline.
- in_range là error vì tốc độ ngoài phạm vi 0–70 m/s là dữ liệu không hợp lệ, cần chặn để tránh ảnh hưởng đến các bước phân tích sau.

Kết quả thí nghiệm break-gate:
- Khi nới max_speed_mps từ 70 lên 1000 m/s, có 6 dòng tốc độ vô lý lọt vào Silver.
- Test in_range_silver_telemetry_speed_mps__70__0 phát hiện 6 dòng vi phạm.
- Kết quả khi nới luật: PASS=5 WARN=1 ERROR=1.
- Sau khi khôi phục luật gốc: PASS=6 WARN=1 ERROR=0.

Vai trò của cổng chất lượng: Cổng chất lượng giúp phát hiện và ngăn dữ liệu không hợp lệ đi tiếp trong pipeline. Thí nghiệm chứng minh rằng dù bước làm sạch bị nới lỏng, test in_range vẫn phát hiện vi phạm và đánh dấu pipeline thất bại. Nhờ đó, lỗi dữ liệu được phát hiện sớm, đảm bảo độ tin cậy của dữ liệu Silver và các báo cáo phía sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

# Trả lời
- Trước khi chạy lại: 1246 dòng.
- Sau hai lần chạy lại: 1246 dòng, fingerprint không đổi (6c7480dfde7e).
- Khi dùng append: 60 khung hình bị ghi thành 120 dòng, dữ liệu bị nhân đôi.
Delta overwrite theo partition an toàn hơn vì chỉ ghi đè dữ liệu của trip cần xử lý, tránh trùng lặp khi chạy lại. Cơ chế giao dịch ACID giúp bảo đảm tính nhất quán: nếu ghi thất bại giữa chừng, dữ liệu cũ vẫn được bảo vệ thay vì để lại trạng thái xóa rồi chưa ghi xong.


### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

# Trả lời
So sánh khung hình đen và khung hình tối:
- Khung đen: gần như không có thông tin hình ảnh, có thể do lỗi camera hoặc mất tín hiệu.
- Khung tối: vẫn có chi tiết nhưng thiếu sáng, ví dụ khi đi qua đường hầm hoặc bóng râm.
Pipeline phát hiện, đánh dấu khung đen; khung tối được gắn cờ nhưng vẫn giữ lại để không mất thông tin hữu ích. Tôi đồng ý với cách xử lý này.

- Số khung bị coi là trùng theo ngưỡng dHash:
Ngưỡng	Drive A	Drive B
0 bit	61	8
2 bit	149	53
6 bit	363	244

- Lựa chọn ngưỡng: chọn ngưỡng 2 bit vì cân bằng giữa phát hiện khung gần trùng và tránh loại nhầm khung có thông tin khác biệt. Ngưỡng 0 quá nghiêm ngặt, còn ngưỡng 6 có thể loại bỏ nhiều khung hình khác nhau.
Ngưỡng còn phụ thuộc vào tốc độ xe: xe chạy chậm hoặc camera đứng hình tạo ra nhiều khung gần giống nhau; xe chạy nhanh khiến cảnh vật thay đổi nhiều hơn. Vì vậy, cần chọn ngưỡng phù hợp để tránh loại bỏ khung hình chứa thông tin mới.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

# Trả lời
Trong 1019 khung dùng được thuộc 26 trip, cách chia ngẫu nhiên theo khung hình làm cả 26 trip xuất hiện ở nhiều split, và cả 26 có khung ở cả train lẫn test. Chia theo trip của pipeline làm 0 trip bị chia cắt. Các khung liên tiếp trong cùng cảnh rất giống nhau; nếu cảnh đã vào train cũng lọt vào test, mô hình có thể nhận ra cùng cảnh thay vì tổng quát hoá, khiến điểm test cao giả tạo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

# Trả lời
`dataset_version` là 12 ký tự đầu của SHA-256 trên nội dung có thứ tự `frame_id|split`, nên định danh nội dung dataset thay vì thời điểm chạy hay số đếm dễ trùng. Bước 4 và lần chạy `step all --reset` đều tạo `cc1d1c40056a`, phù hợp với tính tái lập khi dữ liệu/luật không đổi. MSSV được dùng làm salt khi phân trip vào split; MSSV khác có thể đổi cặp frame/split, vì vậy hash và version cũng đổi dù đầu vào gốc giống nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

# Trả lời
`manifest.json` ghi dataset `cc1d1c40056a`, 1019 ảnh, các luật loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`, và chia per-trip, phân tầng theo drive, thứ tự MD5. Lineage là các phiên bản Delta: `bronze.raw_telemetry_a=0`, `bronze.raw_telemetry_b=0`, `bronze.trips_raw=0`, `bronze.video_files=1`, `silver.frames=25`, `silver.telemetry=0`, `silver.frames_flagged=0`, `silver.frame_telemetry=0`. SHA-256 video đầu vào: drive A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; drive B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Các version, hash, luật và manifest hash (`528c68d2dcd3f3763b511f56da775b3c3b0831e7594a60c94398146a1da1a23c`) giúp xác định lại đúng đầu vào và quy tắc tạo dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.
