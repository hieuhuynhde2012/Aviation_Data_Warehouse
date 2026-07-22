"""Bootstrap Superset assets for the Aviation Data Warehouse project.

Run from the host:
    Get-Content superset/setup_assets.py | docker compose exec -T superset python -
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import create_engine, inspect

from superset.app import create_app


WAREHOUSE_URI = "postgresql+psycopg2://aviation:aviation@postgres-warehouse:5432/aviation_dw"
DATABASE_NAME = "Aviation Warehouse"
DASHBOARD_TITLE = "Aviation Data Warehouse Operations"
ADMIN_USERNAME = "admin"


@dataclass(frozen=True)
class DatasetSpec:
    schema: str
    table: str
    main_dttm_col: str | None = None


DATASETS = [
    DatasetSpec("mart", "mart_sales_performance", "flight_date"),
    DatasetSpec("mart", "mart_booking_trend_daily", "booking_date"),
    DatasetSpec("mart", "mart_booking_status_realtime", "latest_update_time"),
    DatasetSpec("mart", "mart_route_performance"),
    DatasetSpec("mart", "mart_airport_performance"),
    DatasetSpec("mart", "mart_customer_segment"),
    DatasetSpec("mart", "mart_flight_delay_analysis", "flight_date"),
    DatasetSpec("metadata", "dq_record_errors", "created_at"),
    DatasetSpec("metadata", "reconciliation_summary", "created_at"),
    DatasetSpec("metadata", "raw_load_audit", "completed_at"),
    DatasetSpec("quarantine", "invalid_records", "created_at"),
]


CHARTS = [
    {
        "name": "Total Revenue",
        "dataset": ("mart", "mart_sales_performance"),
        "viz_type": "big_number_total",
        "params": {
            "metric": "sum__revenue",
            "time_range": "No filter",
            "y_axis_format": "$,.2f",
        },
        "width": 3,
        "height": 14,
    },
    {
        "name": "Total Bookings",
        "dataset": ("mart", "mart_sales_performance"),
        "viz_type": "big_number_total",
        "params": {
            "metric": "sum__booking_count",
            "time_range": "No filter",
            "y_axis_format": ",d",
        },
        "width": 3,
        "height": 14,
    },
    {
        "name": "Average Ticket Value",
        "dataset": ("mart", "mart_sales_performance"),
        "viz_type": "big_number_total",
        "params": {
            "metric": "avg__average_ticket_value",
            "time_range": "No filter",
            "y_axis_format": "$,.2f",
        },
        "width": 3,
        "height": 14,
    },
    {
        "name": "Quarantined Records",
        "dataset": ("quarantine", "invalid_records"),
        "viz_type": "big_number_total",
        "params": {
            "metric": "count",
            "time_range": "No filter",
            "y_axis_format": ",d",
        },
        "width": 3,
        "height": 14,
    },
    {
        "name": "Top Routes By Revenue",
        "dataset": ("mart", "mart_route_performance"),
        "viz_type": "dist_bar",
        "params": {
            "groupby": ["route"],
            "metrics": ["sum__revenue"],
            "row_limit": 10,
            "order_desc": True,
            "y_axis_format": "$,.2f",
            "show_legend": False,
        },
        "width": 6,
        "height": 32,
    },
    {
        "name": "Booking Trend By Day",
        "dataset": ("mart", "mart_booking_trend_daily"),
        "viz_type": "line",
        "params": {
            "granularity_sqla": "booking_date",
            "time_range": "No filter",
            "metrics": ["sum__booking_count", "sum__confirmed_revenue"],
            "groupby": ["booking_channel"],
            "row_limit": 5000,
            "show_legend": True,
            "x_axis_format": "smart_date",
        },
        "width": 6,
        "height": 32,
    },
    {
        "name": "Booking Status Mix",
        "dataset": ("mart", "mart_booking_status_realtime"),
        "viz_type": "pie",
        "params": {
            "groupby": ["booking_status"],
            "metric": "sum__booking_count",
            "row_limit": 20,
            "donut": True,
            "show_legend": True,
        },
        "width": 4,
        "height": 28,
    },
    {
        "name": "Delay Analysis By Airline",
        "dataset": ("mart", "mart_flight_delay_analysis"),
        "viz_type": "dist_bar",
        "params": {
            "groupby": ["airline"],
            "metrics": ["avg__avg_delay_minutes", "avg__delayed_rate"],
            "row_limit": 15,
            "order_desc": True,
            "show_legend": True,
        },
        "width": 4,
        "height": 28,
    },
    {
        "name": "DQ Errors By Rule",
        "dataset": ("metadata", "dq_record_errors"),
        "viz_type": "dist_bar",
        "params": {
            "groupby": ["rule_name", "severity"],
            "metrics": ["count"],
            "row_limit": 20,
            "order_desc": True,
            "show_legend": True,
        },
        "width": 4,
        "height": 28,
    },
    {
        "name": "Reconciliation Status",
        "dataset": ("metadata", "reconciliation_summary"),
        "viz_type": "table",
        "params": {
            "query_mode": "aggregate",
            "groupby": ["run_id", "source_name", "target_table", "status"],
            "metrics": [
                "sum__source_file_rows",
                "sum__quarantined_rows",
                "sum__expected_raw_rows",
                "sum__actual_raw_rows",
                "sum__difference",
            ],
            "row_limit": 1000,
            "include_search": True,
            "table_timestamp_format": "smart_date",
        },
        "width": 8,
        "height": 34,
    },
    {
        "name": "Quarantine Detail",
        "dataset": ("quarantine", "invalid_records"),
        "viz_type": "table",
        "params": {
            "query_mode": "raw",
            "all_columns": [
                "created_at",
                "run_id",
                "source_name",
                "record_key",
                "reason",
                "file_path",
            ],
            "order_by_cols": ["[\"created_at\", false]"],
            "row_limit": 1000,
            "include_search": True,
            "table_timestamp_format": "smart_date",
        },
        "width": 4,
        "height": 34,
    },
]


def get_admin_user(app):
    return app.appbuilder.sm.find_user(username=ADMIN_USERNAME)


def get_or_create_database(db, Database):
    database = (
        db.session.query(Database)
        .filter(Database.database_name == DATABASE_NAME)
        .one_or_none()
    )
    if database is None:
        database = Database(database_name=DATABASE_NAME, expose_in_sqllab=True)
        db.session.add(database)

    database.sqlalchemy_uri = WAREHOUSE_URI
    database.expose_in_sqllab = True
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_dml = False
    database.extra = json.dumps(
        {
            "metadata_params": {},
            "engine_params": {"connect_args": {"connect_timeout": 10}},
            "schemas_allowed_for_csv_upload": [],
        }
    )
    db.session.commit()
    return database


def metric_expression(prefix: str, column_name: str | None = None) -> str:
    if prefix == "count":
        return "COUNT(*)"
    if column_name is None:
        raise ValueError("column_name is required")
    return f"{prefix.upper()}({column_name})"


def upsert_metric(SqlMetric, dataset, metric_name: str, expression: str) -> None:
    metric = next((item for item in dataset.metrics if item.metric_name == metric_name), None)
    if metric is None:
        metric = SqlMetric(metric_name=metric_name, table=dataset)
        dataset.metrics.append(metric)
    metric.expression = expression
    metric.metric_type = "EXPRESSION"
    metric.verbose_name = metric_name.replace("__", " ").replace("_", " ").title()


def create_metrics(SqlMetric, dataset, columns: list[dict[str, Any]]) -> None:
    numeric_types = {
        "BIGINT",
        "INTEGER",
        "NUMERIC",
        "DOUBLE PRECISION",
        "REAL",
        "SMALLINT",
        "DECIMAL",
    }
    upsert_metric(SqlMetric, dataset, "count", "COUNT(*)")
    for column in columns:
        name = column["name"]
        col_type = column["type"].upper()
        if col_type in numeric_types:
            upsert_metric(SqlMetric, dataset, f"sum__{name}", metric_expression("sum", name))
            upsert_metric(SqlMetric, dataset, f"avg__{name}", metric_expression("avg", name))


def sync_dataset(db, SqlaTable, TableColumn, SqlMetric, database, spec: DatasetSpec, warehouse_columns: list[dict[str, Any]], owner):
    dataset = (
        db.session.query(SqlaTable)
        .filter(
            SqlaTable.database_id == database.id,
            SqlaTable.schema == spec.schema,
            SqlaTable.table_name == spec.table,
        )
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(
            table_name=spec.table,
            schema=spec.schema,
            database=database,
            database_id=database.id,
            uuid=uuid.uuid4(),
        )
        db.session.add(dataset)

    dataset.main_dttm_col = spec.main_dttm_col
    dataset.normalize_columns = True
    dataset.is_sqllab_view = False
    dataset.owners = [owner] if owner else []

    existing = {column.column_name: column for column in dataset.columns}
    seen = set()
    for warehouse_column in warehouse_columns:
        name = warehouse_column["name"]
        seen.add(name)
        column = existing.get(name)
        if column is None:
            column = TableColumn(column_name=name, table=dataset)
            dataset.columns.append(column)
        column.type = warehouse_column["type"]
        column.is_dttm = name == spec.main_dttm_col or "timestamp" in warehouse_column["type"].lower() or warehouse_column["type"].lower() == "date"
        column.groupby = warehouse_column["type"].upper() not in {"JSONB"}
        column.filterable = True
        column.is_active = True

    for column in dataset.columns:
        if column.column_name not in seen:
            column.is_active = False

    create_metrics(SqlMetric, dataset, warehouse_columns)
    db.session.flush()
    return dataset


def inspect_warehouse_columns() -> dict[tuple[str, str], list[dict[str, Any]]]:
    engine = create_engine(WAREHOUSE_URI)
    inspector = inspect(engine)
    output: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for spec in DATASETS:
        output[(spec.schema, spec.table)] = [
            {"name": column["name"], "type": str(column["type"])}
            for column in inspector.get_columns(spec.table, schema=spec.schema)
        ]
    engine.dispose()
    return output


def dataset_ref(dataset) -> str:
    return f"{dataset.id}__table"


def sync_chart(db, Slice, chart_spec: dict[str, Any], datasets: dict[tuple[str, str], Any], owner):
    dataset = datasets[chart_spec["dataset"]]
    params = {
        "datasource": dataset_ref(dataset),
        "viz_type": chart_spec["viz_type"],
        "slice_id": None,
        "adhoc_filters": [],
        "row_limit": 1000,
    }
    params.update(chart_spec["params"])

    chart = db.session.query(Slice).filter(Slice.slice_name == chart_spec["name"]).one_or_none()
    if chart is None:
        chart = Slice(slice_name=chart_spec["name"], uuid=uuid.uuid4())
        db.session.add(chart)

    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.datasource_name = f"{dataset.schema}.{dataset.table_name}"
    chart.viz_type = chart_spec["viz_type"]
    chart.params = json.dumps(params, sort_keys=True)
    chart.query_context = None
    chart.owners = [owner] if owner else []
    db.session.flush()
    return chart


def build_position_json(charts: list[Any], chart_specs: list[dict[str, Any]]) -> str:
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }

    row_index = 0
    current_width = 0
    current_row_id = ""
    for chart, spec in zip(charts, chart_specs):
        width = spec.get("width", 4)
        if not current_row_id or current_width + width > 12:
            row_index += 1
            current_width = 0
            current_row_id = f"ROW-{row_index}"
            position["GRID_ID"]["children"].append(current_row_id)
            position[current_row_id] = {
                "type": "ROW",
                "id": current_row_id,
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }

        chart_id = f"CHART-{chart.id}"
        position[current_row_id]["children"].append(chart_id)
        position[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "meta": {
                "chartId": chart.id,
                "height": spec.get("height", 28),
                "width": width,
            },
        }
        current_width += width

    return json.dumps(position, sort_keys=True)


def sync_dashboard(db, Dashboard, charts: list[Any], chart_specs: list[dict[str, Any]], owner):
    dashboard = (
        db.session.query(Dashboard)
        .filter(Dashboard.dashboard_title == DASHBOARD_TITLE)
        .one_or_none()
    )
    if dashboard is None:
        dashboard = Dashboard(
            dashboard_title=DASHBOARD_TITLE,
            slug="aviation-data-warehouse-operations",
            uuid=uuid.uuid4(),
        )
        db.session.add(dashboard)

    dashboard.published = True
    dashboard.owners = [owner] if owner else []
    dashboard.slices = charts
    dashboard.position_json = build_position_json(charts, chart_specs)
    dashboard.json_metadata = json.dumps(
        {
            "label_colors": {},
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "color_namespace": "Aviation Data Warehouse",
            "default_filters": "{}",
        },
        sort_keys=True,
    )
    db.session.commit()
    return dashboard


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        owner = get_admin_user(app)
        database = get_or_create_database(db, Database)
        warehouse_columns = inspect_warehouse_columns()

        datasets = {}
        for spec in DATASETS:
            datasets[(spec.schema, spec.table)] = sync_dataset(
                db=db,
                SqlaTable=SqlaTable,
                TableColumn=TableColumn,
                SqlMetric=SqlMetric,
                database=database,
                spec=spec,
                warehouse_columns=warehouse_columns[(spec.schema, spec.table)],
                owner=owner,
            )
        db.session.commit()

        charts = [sync_chart(db, Slice, spec, datasets, owner) for spec in CHARTS]
        dashboard = sync_dashboard(db, Dashboard, charts, CHARTS, owner)

        print(f"Database: {database.database_name} (id={database.id})")
        print(f"Datasets: {len(datasets)}")
        print(f"Charts: {len(charts)}")
        print(f"Dashboard: {dashboard.dashboard_title} (id={dashboard.id}, slug={dashboard.slug})")


if __name__ == "__main__":
    main()
