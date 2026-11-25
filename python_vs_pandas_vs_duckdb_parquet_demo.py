"""
Same question, same Parquet file, three tools: plain Python vs pandas vs DuckDB
==============================================================================

This is the Parquet twin of python_vs_pandas_vs_duckdb_demo.py (which uses CSV).
Same data, same question, same three tools. Only the file format changes.

Question: what is the average number of minutes watched per device?

Run it:
    pip install duckdb pandas pyarrow
    python python_vs_pandas_vs_duckdb_parquet_demo.py

Why Parquet is different from CSV:
    CSV stores data row by row, as plain text. To get 2 columns, you must
    read every line and every character, then convert text to numbers.
    Parquet stores data column by column, already typed and compressed.
    To get 2 columns, a tool can jump straight to them and skip the rest.
"""

import os
import time

import duckdb
import pandas as pd
import pyarrow.parquet as pq   # Python has no built-in Parquet reader, so we need pyarrow

NUM_ROWS = 1_000_000
PARQUET_FILE = "playback_demo.parquet"


# ------------------------------------------------------------------
# Step 0: create some fake viewing data (not part of the race)
# ------------------------------------------------------------------
# Exactly the same 6 columns as the CSV demo. Our question only needs 2:
# device and watch_seconds.
print(f"Creating {NUM_ROWS:,} viewing sessions in {PARQUET_FILE}...")

duckdb.sql(f"""
    COPY (
        SELECT
            i                                                  AS session_id,
            (random() * 50000)::INT                            AS user_id,
            (random() * 2000)::INT                             AS title_id,
            ['TV', 'Mobile', 'Laptop', 'Tablet'][1 + (i % 4)]  AS device,
            ['IN', 'US', 'UK', 'BR', 'JP'][1 + (i % 5)]         AS country,
            (60 + random() * 7200)::INT                        AS watch_seconds
        FROM range({NUM_ROWS}) t(i)
    ) TO '{PARQUET_FILE}'
""")

# A Parquet file is binary, so we can't print its lines like a CSV.
# Instead, show the column names and types stored inside it.
print("   Columns:", pq.read_schema(PARQUET_FILE).names)


# ------------------------------------------------------------------
# Way 1: plain Python (with pyarrow to open the file)
# ------------------------------------------------------------------
def plain_python():
    # pyarrow reads ONLY the 2 columns we ask for, already as numbers.
    # .to_pylist() turns each column into a normal Python list.
    table = pq.read_table(PARQUET_FILE, columns=["device", "watch_seconds"])
    devices = table.column("device").to_pylist()          # ["TV", "Mobile", ...]
    seconds = table.column("watch_seconds").to_pylist()   # [703, 6030, ...]

    # From here on it's the same Python loop as the CSV demo,
    # just without the text-to-number conversion.
    total_seconds = {}
    session_count = {}
    for device, secs in zip(devices, seconds):
        if device not in total_seconds:
            total_seconds[device] = 0
            session_count[device] = 0
        total_seconds[device] += secs
        session_count[device] += 1

    # average minutes = total seconds / number of sessions / 60
    answer = {}
    for device in total_seconds:
        answer[device] = total_seconds[device] / session_count[device] / 60
    return answer


# ------------------------------------------------------------------
# Way 2: pandas
# ------------------------------------------------------------------
def with_pandas():
    # Load only the 2 columns we need. Because Parquet is columnar,
    # pandas skips the other 4 columns on disk entirely.
    df = pd.read_parquet(PARQUET_FILE, columns=["device", "watch_seconds"])

    # groupby + mean runs in C code, on the whole column at once.
    avg_seconds = df.groupby("device")["watch_seconds"].mean()

    answer = {}
    for device, seconds in avg_seconds.items():
        answer[device] = seconds / 60
    return answer


# ------------------------------------------------------------------
# Way 3: DuckDB
# ------------------------------------------------------------------
def with_duckdb():
    # Plain SQL, run directly on the Parquet file. DuckDB sees that the query
    # only uses 2 columns and reads only those, in parallel on all CPU cores,
    # in batches of values at a time.
    rows = duckdb.sql(f"""
        SELECT device, AVG(watch_seconds) / 60 AS avg_minutes
        FROM read_parquet('{PARQUET_FILE}')
        GROUP BY device
    """).fetchall()      # a list of tuples: [("TV", 60.2), ("Mobile", 59.9), ...]

    answer = {}
    for device, minutes in rows:
        answer[device] = minutes
    return answer


# ------------------------------------------------------------------
# The race: run each way once, time it, print its answer
# ------------------------------------------------------------------
def run_and_time(name, function):
    start = time.perf_counter()
    answer = function()
    milliseconds = (time.perf_counter() - start) * 1000

    # Sort by device name and show 1 decimal, so the three answers line up.
    answer_text = ", ".join(f"{d}: {answer[d]:.1f}" for d in sorted(answer))
    print(f"{name:<14} {milliseconds:8.1f} ms    {answer_text}")


print("\nAverage minutes watched per device (all three read the same Parquet file)\n")
run_and_time("Plain Python", plain_python)
run_and_time("pandas", with_pandas)
run_and_time("DuckDB", with_duckdb)

# Clean up the demo file.
os.remove(PARQUET_FILE)
