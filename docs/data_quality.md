# Data Quality, Quarantine, CDC, And Reconciliation

This project intentionally models production-style data engineering controls instead of silently cleaning data inside SQL.

## Quality Gates

1. File schema validation
   - Required columns are checked before load.
   - Zero-row files fail validation.

2. Record-level validation
   - Hard errors are quarantined before raw warehouse load.
   - Soft errors are logged and corrected in dbt staging.

3. Controlled vocabulary standardization
   - Booking statuses such as `CONF`, `Booked`, `VOID`, and `PEND` are mapped to canonical values.
   - Payment statuses such as `SUCCESS`, `DECLINED`, and `CHARGEBACK` are mapped to canonical values.
   - Channels, payment methods, customer segments, airport codes, and currency values are normalized.

4. Cross-source validation
   - Payments referencing quarantined or missing bookings are also quarantined.
   - This prevents orphan facts from reaching dbt relationship tests.

## Error Handling

Record-level DQ events are written to:

- File: `data/quality_reports/record_errors.csv`
- Table: `metadata.dq_record_errors`

Invalid records are written to:

- Files: `data/quarantine/<source>/invalid_records.csv`
- Table: `quarantine.invalid_records`

Examples of logged rules:

- `required_not_null`
- `valid_date`
- `valid_numeric`
- `controlled_vocabulary`
- `standardization_required`
- `duplicate_event`
- `relationship_missing`
- `refund_status_mismatch`

## Quarantine Policy

Hard-error records are not loaded into raw fact sources:

- Missing required business keys.
- Invalid dates.
- Invalid numeric values.
- Payment records whose booking failed DQ or does not exist.

Soft-error records are loaded and standardized later:

- Mixed casing and whitespace.
- Known status synonyms.
- Negative ticket price corrections.
- Missing optional customer IDs.
- Duplicate CDC-style events.

## CDC And Idempotent Loads

The raw loader records each file checksum in `metadata.raw_load_audit`.

Behavior:

- Same checksum rerun: skip load, insert zero rows.
- Same logical partition with new checksum: replace that partition/source slice.
- Duplicate booking/payment events: loaded into raw, deduped in staging by keeping latest `updated_at`.
- Incremental dbt models use partition delete+insert hooks so reruns/backfills replace impacted partitions correctly.

## Reconciliation

The pipeline writes source-to-raw reconciliation results to `metadata.reconciliation_summary`.

Current validation example:

| Source | Source rows | Quarantined | Expected raw | Actual raw | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| bts | 597,919 | 0 | 597,919 | 597,919 | PASSED |
| airports | 85,658 | 0 | 85,658 | 85,658 | PASSED |
| bookings | 103,022 | 322 | 102,700 | 102,700 | PASSED |
| payments | 102,981 | 602 | 102,379 | 102,379 | PASSED |

Downstream dbt tests then verify canonical staging/fact integrity, including uniqueness, non-null checks, accepted values, non-negative values, and payment-to-booking relationships.
