"""
Same question, same CSV file, three tools: plain Python vs pandas vs DuckDB
=========================================================================

Question: what is the average number of minutes watched per device?

Run it:
    pip install duckdb pandas
    python python_vs_pandas_vs_duckdb_demo.py

All three tools read the SAME CSV file and give the SAME answer.
The only thing that changes is the tool, so any difference in time
comes from how each tool does the work. (Exact numbers depend on your
machine, but the order almost never changes.)
"""

import csv
import os
import time

import duckdb
import pandas as pd

NUM_ROWS = 1_000_000
CSV_FILE = "playback_demo.csv"


# ------------------------------------------------------------------
# Step 0: create some fake viewing data (not part of the race)
# ------------------------------------------------------------------
# Each row is one viewing session. There are 6 columns, but our question
# only needs 2 of them: device and watch_seconds.
# We use DuckDB here only because it can write a million rows in a second.
print(f"Creating {NUM_ROWS:,} viewing sessions in {CSV_FILE}...")

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
    ) TO '{CSV_FILE}' (HEADER)
""")

# Show the first 3 lines so everyone sees what the file looks like.
with open(CSV_FILE) as f:
    for _ in range(3):
        print("   " + f.readline().strip())


# ------------------------------------------------------------------
# Way 1: plain Python
# ------------------------------------------------------------------
def plain_python():
    # Running total and row count for each device, e.g. {"TV": 123456, ...}
    total_seconds = {}
    session_count = {}

    with open(CSV_FILE, newline="") as f:
        # DictReader gives one row at a time as a dict with ALL 6 columns,
        # even though we only use 2 of them.
        for row in csv.DictReader(f):
            device = row["device"]
            # A CSV is just text, so every number must be converted, row by row.
            seconds = int(row["watch_seconds"])

            if device not in total_seconds:
                total_seconds[device] = 0
                session_count[device] = 0
            total_seconds[device] += seconds
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
    # Read the CSV into memory as a DataFrame, keeping only the 2 columns we need.
    # The file is still text, so pandas must scan every line to find them,
    # but the parsing happens in C code, not a Python loop.
    df = pd.read_csv(CSV_FILE, usecols=["device", "watch_seconds"])

    # groupby + mean also runs in C code, on the whole column at once.
    # Result: a Series like  TV -> 3612.4,  Mobile -> 3598.1, ...
    avg_seconds = df.groupby("device")["watch_seconds"].mean()

    answer = {}
    for device, seconds in avg_seconds.items():
        answer[device] = seconds / 60
    return answer


# ------------------------------------------------------------------
# Way 3: DuckDB
# ------------------------------------------------------------------
def with_duckdb():
    # Plain SQL, run directly on the CSV file. Nothing is loaded first.
    # DuckDB splits the file across all your CPU cores, parses it in parallel,
    # and works on batches of values at a time instead of one row at a time.
    rows = duckdb.sql(f"""
        SELECT device, AVG(watch_seconds) / 60 AS avg_minutes
        FROM read_csv('{CSV_FILE}')
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


print("\nAverage minutes watched per device (all three read the same CSV)\n")
run_and_time("Plain Python", plain_python)
run_and_time("pandas", with_pandas)
run_and_time("DuckDB", with_duckdb)

# Clean up the demo file.
os.remove(CSV_FILE)
