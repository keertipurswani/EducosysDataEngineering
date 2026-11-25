"""
CSV vs Parquet: same data, two file formats
===========================================

Run it:
    pip install duckdb pandas pyarrow
    python csv_vs_parquet_demo.py

We save the SAME 1 million rows twice, once as CSV and once as Parquet,
then compare:
    1. File size        how much disk space each one takes
    2. What's inside    plain text vs stored column types
    3. Read speed       the same question, the same tool, only the format changes

The key difference:
    CSV stores data ROW by row, as plain text.
        session_id,user_id,title_id,device,country,watch_seconds
        0,4762,1363,TV,IN,703
        1,40990,1406,Mobile,US,6030
    To read 2 columns, you still have to read every character of every line.

    Parquet stores data COLUMN by column, in binary, with types, compressed.
        session_id:    0, 1, 2, 3, ...
        device:        TV, Mobile, Laptop, Tablet, ...
        watch_seconds: 703, 6030, ...
    To read 2 columns, a tool jumps straight to them and skips the rest.
"""

import os
import time

import duckdb
import pandas as pd
import pyarrow.parquet as pq

NUM_ROWS = 1_000_000
CSV_FILE = "playback_demo.csv"
PARQUET_FILE = "playback_demo.parquet"


# ------------------------------------------------------------------
# Step 0: create the data and save it in both formats
# ------------------------------------------------------------------
# 6 columns, 1 million viewing sessions. The same rows go into both files.
print(f"Creating {NUM_ROWS:,} viewing sessions...")

duckdb.sql(f"""
    CREATE TABLE sessions AS
    SELECT
        i                                                  AS session_id,
        (random() * 50000)::INT                            AS user_id,
        (random() * 2000)::INT                             AS title_id,
        ['TV', 'Mobile', 'Laptop', 'Tablet'][1 + (i % 4)]  AS device,
        ['IN', 'US', 'UK', 'BR', 'JP'][1 + (i % 5)]         AS country,
        (60 + random() * 7200)::INT                        AS watch_seconds
    FROM range({NUM_ROWS}) t(i)
""")
duckdb.sql(f"COPY sessions TO '{CSV_FILE}' (HEADER)")
duckdb.sql(f"COPY sessions TO '{PARQUET_FILE}'")


# ------------------------------------------------------------------
# 1. File size
# ------------------------------------------------------------------
# Parquet is smaller because it stores each column together (similar values
# sit next to each other, so they compress well) and stores numbers as
# numbers, not as text characters.
csv_mb = os.path.getsize(CSV_FILE) / 1_000_000
parquet_mb = os.path.getsize(PARQUET_FILE) / 1_000_000

print("\n1. FILE SIZE")
print(f"   CSV:     {csv_mb:6.1f} MB")
print(f"   Parquet: {parquet_mb:6.1f} MB   ({csv_mb / parquet_mb:.1f}x smaller)")


# ------------------------------------------------------------------
# 2. What's inside
# ------------------------------------------------------------------
# CSV: just lines of text. There is no schema, so "703" is only text until
# a tool guesses that it's a number.
print("\n2. WHAT'S INSIDE")
print("   CSV (first 3 lines, plain text, no types):")
with open(CSV_FILE) as f:
    for _ in range(3):
        print("      " + f.readline().strip())

# Parquet: binary, so we can't print lines. But the file itself stores
# every column's name AND type, so no tool ever has to guess.
print("   Parquet (column names and types stored inside the file):")
for field in pq.read_schema(PARQUET_FILE):
    print(f"      {field.name:<14} {field.type}")


# ------------------------------------------------------------------
# 3. Read speed: same question, same tool, only the format changes
# ------------------------------------------------------------------
# Question: average minutes watched per device.
# It needs only 2 of the 6 columns: device and watch_seconds.
def time_it(function):
    """Run function() once and return how long it took, in milliseconds."""
    start = time.perf_counter()
    function()
    return (time.perf_counter() - start) * 1000


QUERY = """
    SELECT device, AVG(watch_seconds) / 60 AS avg_minutes
    FROM {source}
    GROUP BY device
"""

# DuckDB on each file. The SQL is identical; only the FROM part changes.
duckdb_csv = time_it(lambda: duckdb.sql(QUERY.format(source=f"read_csv('{CSV_FILE}')")).fetchall())
duckdb_parquet = time_it(lambda: duckdb.sql(QUERY.format(source=f"read_parquet('{PARQUET_FILE}')")).fetchall())

# pandas on each file. Both read only the 2 columns we need,
# but with CSV pandas still has to scan every line to find them.
pandas_csv = time_it(lambda: pd.read_csv(CSV_FILE, usecols=["device", "watch_seconds"])
                     .groupby("device")["watch_seconds"].mean())
pandas_parquet = time_it(lambda: pd.read_parquet(PARQUET_FILE, columns=["device", "watch_seconds"])
                         .groupby("device")["watch_seconds"].mean())

print("\n3. READ SPEED: average minutes watched per device")
print(f"   {'':<8} {'CSV':>10} {'Parquet':>10}   Parquet is")
print(f"   {'DuckDB':<8} {duckdb_csv:7.1f} ms {duckdb_parquet:7.1f} ms   {duckdb_csv / duckdb_parquet:.0f}x faster")
print(f"   {'pandas':<8} {pandas_csv:7.1f} ms {pandas_parquet:7.1f} ms   {pandas_csv / pandas_parquet:.0f}x faster")

# Clean up the demo files.
os.remove(CSV_FILE)
os.remove(PARQUET_FILE)
