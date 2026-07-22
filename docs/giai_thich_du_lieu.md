# Giai Thich Du Lieu Project Aviation Data Warehouse

Tai lieu nay giai thich bo du lieu trong project: du lieu den tu dau, co y nghia gi, bi ban nhu the nao, va sau khi di qua pipeline thi thanh cac bang nao de phan tich.

## 1. Muc Tieu Du Lieu

Project mo phong mot he thong data warehouse cho nganh hang khong, ket hop 3 nhom du lieu chinh va 1 lop xu ly Spark:

| Nhom du lieu | Muc dich |
| --- | --- |
| Flight operations | Phan tich chuyen bay, delay, cancellation, route, airline, airport |
| Airport reference | Lam dimension san bay, quoc gia, vung |
| Booking/payment business events | Phan tich doanh thu, kenh dat ve, trang thai booking/payment, customer segment |
| Spark feature outputs | Feature/aggregate lon theo route va airline |

Du lieu booking va payment la synthetic vi du lieu thuong mai that cua hang bay khong cong khai. Tuy nhien, generator duoc thiet ke de bam vao cac flight/route that tu BTS, nen project van co tinh thuc te.

## 2. Nguon Du Lieu

### BTS Airline On-Time Performance

Day la du lieu cong khai ve hoat dong chuyen bay.

File local:

```text
data/input/bts/bts_on_time_2026_04.csv
```

Dung de tra loi cac cau hoi:

- Chuyen bay nao bi tre?
- Route nao co delay cao?
- Airline nao co van de ve delay/cancellation?
- San bay nao co volume va operational issue lon?

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `flight_date` | Ngay bay |
| `reporting_airline` | Ma hang bay |
| `flight_number_reporting_airline` | So hieu chuyen bay |
| `origin`, `dest` | San bay di/den |
| `dep_delay`, `arr_delay` | So phut delay luc khoi hanh/den |
| `cancelled`, `diverted` | Co bi huy/chuyen huong khong |
| `distance` | Khoang cach duong bay |
| `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay` | Nguyen nhan delay |

### OurAirports

Day la du lieu reference ve san bay/quoc gia/vung.

File local:

```text
data/input/ourairports/airports.csv
data/input/ourairports/countries.csv
data/input/ourairports/regions.csv
```

Dung de enrich du lieu flight:

- Ten day du cua san bay
- Vi tri dia ly
- Quoc gia/vung
- Loai san bay

Cot quan trong trong `airports.csv`:

| Cot | Y nghia |
| --- | --- |
| `ident` | Ma dinh danh san bay |
| `iata_code` | Ma IATA, vi du JFK, LAX |
| `name` | Ten san bay |
| `type` | Loai san bay |
| `latitude_deg`, `longitude_deg` | Toa do |
| `iso_country`, `iso_region` | Quoc gia/vung |
| `municipality` | Thanh pho/khu vuc |

### Synthetic Bookings

File local:

```text
data/generated/bookings/bookings.csv
```

Bang nay mo phong giao dich dat ve cua khach hang.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `booking_id` | Ma booking |
| `customer_id` | Ma khach hang |
| `flight_id` | Ma chuyen bay synthetic, gan voi BTS flight |
| `flight_date` | Ngay bay |
| `airline` | Hang bay |
| `origin_airport`, `destination_airport` | San bay di/den |
| `route` | Chang bay, vi du `JFK-LAX` |
| `booking_time` | Thoi diem dat ve |
| `booking_channel` | Kenh dat ve: web, mobile, agency, call center |
| `customer_segment` | Phan khuc khach hang |
| `ticket_price` | Gia ve |
| `currency` | Don vi tien |
| `booking_status` | Trang thai booking |
| `payment_status` | Trang thai thanh toan |
| `is_refunded` | Co refund khong |
| `created_at`, `updated_at` | Thoi diem tao/cap nhat record |

### Synthetic Payments

File local:

```text
data/generated/payments/payments.csv
```

Bang nay mo phong giao dich thanh toan cho booking.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `payment_id` | Ma payment |
| `booking_id` | Booking ma payment nay thuoc ve |
| `payment_time` | Thoi diem thanh toan |
| `payment_method` | Phuong thuc thanh toan |
| `payment_status` | Trang thai thanh toan |
| `amount` | So tien thanh toan |
| `currency` | Don vi tien |
| `refund_amount` | So tien hoan |
| `created_at`, `updated_at` | Thoi diem tao/cap nhat record |

## 3. Du Lieu Bi Ban Co Chu Dich

Project co chu dich tao du lieu khong qua sach de the hien vai tro Data Engineer.

Vi du dirty data:

| Loai van de | Vi du | Xu ly |
| --- | --- | --- |
| Sai casing/spacing | ` paid `, `mobile app`, ` jfk ` | Trim, uppercase, map ve canonical value |
| Synonym status | `CONF`, `Booked`, `VOID`, `SUCCESS`, `CHARGEBACK` | Chuan hoa controlled vocabulary |
| Duplicate event | Cung `booking_id` nhung `updated_at` moi hon | CDC dedupe, giu ban moi nhat |
| Missing optional field | Rong `customer_id` | Doi thanh `UNKNOWN_CUSTOMER` |
| Gia tri am | `ticket_price < 0` | Sua bang `abs()` neu la soft issue |
| Sai kieu du lieu nghiem trong | `ticket_price = not-a-number`, `flight_date = not-a-date` | Quarantine |
| Mat khoa business key | Rong `booking_id`, `payment_id` | Quarantine |
| Payment mo coi | Payment tro den booking khong ton tai/bi quarantine | Quarantine payment |
| Refund mismatch | Status/amount refund khong hop ly | Log DQ warning |

## 4. Controlled Vocabulary

Pipeline chuan hoa cac gia tri ve bo gia tri hop le.

### Booking status

Gia tri hop le sau cleaning:

```text
CONFIRMED
CANCELLED
PENDING
```

Vi du mapping:

| Raw value | Canonical |
| --- | --- |
| `CONF`, `Booked`, `complete` | `CONFIRMED` |
| `VOID`, `CANCELED`, `cncl` | `CANCELLED` |
| `PEND`, `IN_PROGRESS`, `awaiting_payment` | `PENDING` |

### Payment status

Gia tri hop le sau cleaning:

```text
PAID
FAILED
REFUNDED
```

Vi du mapping:

| Raw value | Canonical |
| --- | --- |
| `SUCCESS`, `SETTLED`, `paid` | `PAID` |
| `DECLINED`, `ERROR`, `fail` | `FAILED` |
| `REFUND`, `CHARGEBACK`, `refunded` | `REFUNDED` |

## 5. Cac Layer Du Lieu

### Raw layer

Schema:

```text
raw
```

Bang chinh:

```text
raw.raw_bts_flights
raw.raw_airports
raw.raw_countries
raw.raw_regions
raw.raw_bookings
raw.raw_payments
raw.raw_booking_events_stream
raw.raw_payment_events_stream
```

Raw layer giu du lieu gan voi source nhat co the. Record bi loi nghiem trong se khong vao raw booking/payment ma vao quarantine.

### Staging layer

Schema:

```text
staging
```

Bang/model chinh:

```text
staging.stg_flights
staging.stg_airports
staging.stg_bookings
staging.stg_payments
```

Staging lam cac viec:

- Cast kieu du lieu
- Trim/uppercase code
- Chuan hoa status/channel/payment method
- Dedupe CDC event
- Sua soft issue
- Tao gia tri mac dinh nhu `UNKNOWN_CUSTOMER`

### Intermediate layer

Schema:

```text
intermediate
```

Dung de tao cac join/logic trung gian cho fact va mart.

Vi du:

```text
intermediate.int_route_daily
intermediate.int_flight_booking_bridge
intermediate.int_customer_booking
```

### Mart layer

Schema:

```text
mart
```

Day la layer dung cho analytics, BI, Superset.

Dimensions:

```text
mart.dim_airport
mart.dim_airline
mart.dim_route
mart.dim_customer
mart.dim_date
```

Facts:

```text
mart.fact_flight_status
mart.fact_booking
mart.fact_payment
```

Dashboard marts:

```text
mart.mart_sales_performance
mart.mart_booking_status_realtime
mart.mart_booking_trend_daily
mart.mart_route_performance
mart.mart_airport_performance
mart.mart_customer_segment
mart.mart_flight_delay_analysis
```

## 6. Bang Chat Luong Du Lieu Va Quarantine

### metadata.dq_record_errors

Luu loi DQ theo tung record.

Dung de tra loi:

- Record nao loi?
- Loi cot nao?
- Severity la WARN hay ERROR?
- Rule nao bi violate?
- Pipeline da lam gi voi record do?

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `error_id` | ID loi |
| `run_id` | Lan chay pipeline |
| `source_name` | Nguon du lieu |
| `row_number` | Dong bi loi |
| `record_key` | Business key |
| `severity` | WARN/ERROR |
| `rule_name` | Rule DQ |
| `column_name` | Cot bi loi |
| `bad_value` | Gia tri loi |
| `action_taken` | Hanh dong xu ly |

### quarantine.invalid_records

Luu record loi nghiem trong.

Khac voi drop im lang, project giu lai raw payload de debug/replay.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `quarantine_id` | ID quarantine |
| `run_id` | Lan chay pipeline |
| `source_name` | Nguon |
| `record_key` | Business key |
| `reason` | Ly do quarantine |
| `raw_record` | Toan bo record dang JSON |

### metadata.reconciliation_summary

So sanh row count giua source va target raw.

Cong thuc:

```text
expected_raw_rows = source_file_rows - quarantined_rows
```

Neu:

```text
expected_raw_rows = actual_raw_rows
```

thi status la:

```text
PASSED
```

### metadata.raw_load_audit

Theo doi idempotent load.

Dung de chung minh:

- File da load chua?
- Checksum co trung khong?
- Rerun co skip file cu khong?
- Insert bao nhieu row?
- Skip bao nhieu row?

## 7. Streaming Data Kafka

Kafka sidecar mo phong booking/payment event gan realtime.

Topics:

```text
booking_events
payment_events
```

Raw stream tables:

```text
raw.raw_booking_events_stream
raw.raw_payment_events_stream
```

Moi message Kafka co envelope:

| Field | Y nghia |
| --- | --- |
| `event_id` | Key chong duplicate |
| `event_type` | Loai event |
| `event_time` | Thoi gian nghiep vu |
| `source_system` | He thong nguon |
| `schema_version` | Version schema |
| `entity_key` | Key de partition/order |
| `payload` | Du lieu goc |

Vi du event type:

```text
BOOKING_CREATED
BOOKING_UPDATED
BOOKING_CANCELLED
PAYMENT_CAPTURED
PAYMENT_FAILED
PAYMENT_REFUNDED
```

Streaming co cac control:

- Kafka key theo `booking_id`/`payment_id`
- `event_id` primary key de idempotent consumer
- Duplicate event se bi skip
- Invalid event vao `metadata.streaming_dlq`
- Count duoc reconcile trong `metadata.streaming_reconciliation`

## 8. Cac Cau Hoi Co The Phan Tich

## 8. Spark Feature Processing

Project co them Spark de lam noi bat nang luc xu ly du lieu lon/distributed processing.

Spark khong thay dbt. Vai tro cua Spark trong project nay la:

- Doc file BTS CSV lon tu `data/input/bts`.
- Doc booking CSV tu `data/generated/bookings`.
- Tinh feature/aggregate theo route va airline.
- Ghi output nguoc lai PostgreSQL warehouse.
- Ghi audit cho moi lan chay Spark job.

Spark job chinh:

```text
spark/jobs/aviation_feature_job.py
```

Bang output:

```text
mart.spark_route_delay_features
mart.spark_route_booking_features
metadata.spark_job_audit
```

### mart.spark_route_delay_features

Bang nay gom feature ve delay theo route va airline.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `route` | Chang bay |
| `airline` | Hang bay |
| `flight_count` | So chuyen bay |
| `cancelled_count` | So chuyen bi huy |
| `diverted_count` | So chuyen bi divert |
| `avg_dep_delay_minutes` | Delay khoi hanh trung binh |
| `avg_arr_delay_minutes` | Delay den trung binh |
| `p95_arr_delay_minutes` | P95 arrival delay |
| `carrier_delay_minutes` | Tong delay do carrier |
| `weather_delay_minutes` | Tong delay do thoi tiet |
| `nas_delay_minutes` | Tong delay do NAS |
| `late_aircraft_delay_minutes` | Tong delay do tau bay den muon |

### mart.spark_route_booking_features

Bang nay gom feature booking/revenue theo route.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `route` | Chang bay |
| `booking_event_count` | So booking events |
| `confirmed_booking_events` | So booking confirmed |
| `cancelled_booking_events` | So booking cancelled |
| `distinct_customers` | So khach hang distinct |
| `total_ticket_value` | Tong gia tri ticket |
| `avg_ticket_price` | Gia ve trung binh |
| `web_booking_events` | Booking tu web |
| `mobile_booking_events` | Booking tu mobile |

### metadata.spark_job_audit

Bang audit de biet Spark job co chay thanh cong khong.

Cot quan trong:

| Cot | Y nghia |
| --- | --- |
| `job_run_id` | ID moi lan chay |
| `job_name` | Ten Spark job |
| `source_flight_rows` | So row flight input |
| `source_booking_rows` | So row booking input |
| `output_route_delay_rows` | So row output delay feature |
| `output_route_booking_rows` | So row output booking feature |
| `status` | SUCCESS/FAILED |
| `error_message` | Loi neu co |

Lenh chay Spark:

```powershell
scripts/run_spark_feature_job.ps1
```

Hoac chay manual:

```powershell
docker compose up -d spark-master spark-worker spark-submit
Get-Content warehouse/05_spark_tables.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
docker compose exec spark-submit spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark/work-dir/jobs/aviation_feature_job.py
```

Khi phong van co the noi:

```text
Spark duoc dung de xu ly file flight operation lon va tao feature table theo route/airline. dbt van phu trach warehouse modeling, con Spark phu trach distributed feature processing.
```

## 9. Cac Cau Hoi Co The Phan Tich

Business:

- Route nao co doanh thu cao nhat?
- Kenh booking nao mang lai nhieu booking/doanh thu nhat?
- Customer segment nao co spending cao?
- Payment failed rate co cao khong?
- Refund amount theo ngay/route nhu the nao?

Operations:

- Airline nao delay nhieu?
- San bay nao co delay/cancellation cao?
- Nguyen nhan delay chu yeu la carrier/weather/NAS hay late aircraft?
- Route nao vua co revenue cao vua co delay cao?

Data Engineering:

- Bao nhieu record bi standardization?
- Bao nhieu record bi quarantine?
- Reconciliation co pass khong?
- Rerun co bi duplicate khong?
- Kafka consumer co idempotent khong?
- DLQ co record khong?

## 10. Query Nen Dung De Kiem Tra Du Lieu

Chay toan bo demo query:

```powershell
Get-Content sql/demo_queries.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
```

Mot so query quan trong:

```sql
select source_name, source_file_rows, quarantined_rows, expected_raw_rows, actual_raw_rows, status
from metadata.reconciliation_summary
order by created_at desc;
```

```sql
select severity, rule_name, count(*) as record_count
from metadata.dq_record_errors
group by severity, rule_name
order by record_count desc;
```

```sql
select source_name, reason, count(*) as record_count
from quarantine.invalid_records
group by source_name, reason
order by record_count desc;
```

```sql
select run_id, topic_name, produced_events, consumed_events, duplicate_events, dlq_events, status
from metadata.streaming_reconciliation
order by created_at desc;
```

```sql
select job_name, status, source_flight_rows, source_booking_rows, output_route_delay_rows, output_route_booking_rows
from metadata.spark_job_audit
order by completed_at desc;
```

## 11. Tom Tat De Noi Khi Phong Van

Day la project aviation data warehouse co ca batch va streaming:

- Batch pipeline xu ly BTS, OurAirports, bookings, payments.
- Kafka sidecar mo phong booking/payment event realtime.
- Spark xu ly feature engineering tren file flight CSV lon.
- Du lieu co chu dich dirty de the hien cleaning va data quality.
- Hard errors vao quarantine, soft errors duoc log va standardize.
- Raw load idempotent bang checksum.
- CDC duplicate duoc dedupe theo `updated_at`.
- Source-to-target reconciliation dam bao row count dung.
- dbt build facts/dimensions/marts va test constraints.
- Superset dung mart layer de dashboard.

Noi ngan gon:

```text
Project nay khong chi load data vao warehouse, ma co day du control de du lieu tro nen dang tin cay: DQ, quarantine, idempotency, CDC, reconciliation, dbt tests, BI dashboard va Kafka streaming sidecar.
```
