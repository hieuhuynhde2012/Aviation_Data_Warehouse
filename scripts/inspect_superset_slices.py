import sqlite3


conn = sqlite3.connect("/app/superset_home/superset.db")
cur = conn.cursor()

cur.execute(
    """
    select id, slice_name, viz_type, params
    from slices
    where slice_name in (
        'Total Revenue',
        'Total Bookings',
        'Average Ticket Value',
        'Quarantined Records'
    )
    order by id
    """
)

for row in cur.fetchall():
    print(f"ID={row[0]}")
    print(f"NAME={row[1]}")
    print(f"VIZ={row[2]}")
    print(row[3])
    print("---")

conn.close()
