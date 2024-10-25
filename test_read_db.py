import sqlite3

conn = sqlite3.connect('mqtt_data.db')
c = conn.cursor()

c.execute("SELECT * FROM mqtt_data")
rows = c.fetchall()
num_rows = len(rows)
print("Number of rows:", num_rows)

for row in rows:
    print(row)

conn.close()
