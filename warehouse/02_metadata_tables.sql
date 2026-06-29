create table if not exists metadata.pipeline_file_registry (
    file_id text primary key,
    source_name text not null,
    file_path text not null,
    partition_key text,
    file_size_bytes bigint,
    row_count bigint,
    checksum_md5 text,
    load_status text not null,
    started_at timestamp,
    completed_at timestamp,
    error_message text
);

create table if not exists metadata.pipeline_run_log (
    run_id text primary key,
    dag_id text,
    execution_date timestamp,
    source_count integer,
    total_input_rows bigint,
    total_loaded_rows bigint,
    dbt_status text,
    test_status text,
    runtime_seconds numeric,
    created_at timestamp default current_timestamp
);
