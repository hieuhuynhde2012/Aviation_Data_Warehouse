import sqlite3


conn = sqlite3.connect("/app/superset_home/superset.db")
cur = conn.cursor()

cur.execute("select name from sqlite_master where type='table' order by name")
print("TABLES")
for row in cur.fetchall():
    print(row[0])

print("\nCHART_LIKE_TABLES")
cur.execute(
    """
    select name
    from sqlite_master
    where type='table'
      and (name like '%chart%' or name like '%slice%' or name like '%dashboard%')
    order by name
    """
)
for row in cur.fetchall():
    print(row[0])

conn.close()
