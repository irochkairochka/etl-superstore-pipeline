import sqlite3
from pathlib import Path

db_path = Path("data/processed/superstore.db")
conn = sqlite3.connect(db_path)
cur = conn.cursor()

rows = cur.execute('SELECT "Order ID", Sales, Year, Month, Revenue_per_order FROM sales LIMIT 5').fetchall()
print("TOP 5 rows:")
for r in rows:
    print(r)

agg = cur.execute("SELECT Year, ROUND(SUM(Sales), 2) as total_sales FROM sales GROUP BY Year").fetchall()
print("\nSales by Year:")
for a in agg:
    print(a)

conn.close() 
