import json
import sqlite3


TARGETS = {
    "Total Revenue": "$,.2f",
    "Total Bookings": ",d",
    "Average Ticket Value": "$,.2f",
    "Quarantined Records": ",d",
}


conn = sqlite3.connect("/app/superset_home/superset.db")
cur = conn.cursor()

cur.execute(
    """
    select id, slice_name, params
    from slices
    where slice_name in (
        'Total Revenue',
        'Total Bookings',
        'Average Ticket Value',
        'Quarantined Records'
    )
    """
)

rows = cur.fetchall()
for slice_id, slice_name, params_json in rows:
    params = json.loads(params_json)
    params["viz_type"] = "big_number_total"
    params["time_range"] = "No filter"
    params.pop("compare_lag", None)
    params.pop("compare_suffix", None)
    params.pop("show_trend_line", None)
    params["slice_id"] = slice_id
    params["y_axis_format"] = TARGETS[slice_name]

    cur.execute(
        """
        update slices
        set viz_type = ?, params = ?
        where id = ?
        """,
        ("big_number_total", json.dumps(params, sort_keys=True), slice_id),
    )

conn.commit()

print(f"Updated {len(rows)} slices")
for slice_id, slice_name, _ in rows:
    print(f"{slice_id}: {slice_name}")

conn.close()
