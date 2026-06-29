create table if not exists metadata.dq_record_errors (
    error_id text primary key,
    run_id text,
    source_name text not null,
    file_path text not null,
    row_number bigint,
    record_key text,
    severity text not null,
    rule_name text not null,
    column_name text,
    bad_value text,
    error_message text not null,
    action_taken text not null,
    created_at timestamp default current_timestamp
);

create table if not exists quarantine.invalid_records (
    quarantine_id text primary key,
    run_id text,
    source_name text not null,
    file_path text not null,
    row_number bigint,
    record_key text,
    reason text not null,
    raw_record jsonb not null,
    created_at timestamp default current_timestamp
);

create table if not exists metadata.reconciliation_summary (
    reconciliation_id text primary key,
    run_id text,
    source_name text not null,
    source_file_rows bigint not null,
    quarantined_rows bigint not null,
    expected_raw_rows bigint not null,
    actual_raw_rows bigint not null,
    target_table text not null,
    status text not null,
    difference bigint not null,
    created_at timestamp default current_timestamp
);

create table if not exists metadata.raw_load_audit (
    load_audit_id text primary key,
    run_id text,
    source_name text not null,
    target_table text not null,
    file_path text not null,
    checksum_md5 text not null,
    source_rows bigint not null,
    valid_rows bigint not null,
    quarantined_rows bigint not null,
    inserted_rows bigint not null,
    skipped_rows bigint not null,
    load_status text not null,
    started_at timestamp,
    completed_at timestamp,
    error_message text
);
