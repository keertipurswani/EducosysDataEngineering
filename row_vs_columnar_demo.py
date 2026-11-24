"""
Row store (SQLite) vs Column store (DuckDB): OLTP vs OLAP demo
===============================================================

How to run:
    pip install -r requirements.txt
    python row_vs_columnar_demo.py            # default: 5 million rows
    python row_vs_columnar_demo.py 2000000    # or choose your own row count

What it shows:
    - OLTP (many tiny single-row operations)   -> SQLite (row store) wins
    - OLAP (few big scans + aggregations)      -> DuckDB (column store) wins

Why the comparison is fair:
    Both are EMBEDDED databases: no server, no network. Each one is a library
    running inside this Python process and storing data in a single file.
    Same data, same machine, same process. The only real difference is how
    each engine lays out and processes the data.
"""

import os          # file sizes, deleting old database files
import sys         # reading the row count from the command line
import time        # timing each query
import random      # picking random order_ids for the OLTP tests
import sqlite3     # SQLite: built into Python's standard library
import duckdb      # DuckDB: third-party, installed from requirements.txt


# ------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------
# Number of rows: use the command-line argument if given, otherwise 5M.
# Below ~100K rows both databases finish in milliseconds and the OLAP gap
# disappears, so keep this in the millions.
N = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000

# The two database files that will be created in the current folder.
SQLITE_FILE = "orders_row.sqlite"
DUCK_FILE = "orders_col.duckdb"

# How many single-row operations each OLTP test performs.
# One lookup takes ~1 ms in both databases, too small to compare,
# so we repeat it 1000 times to make the difference visible.
OLTP_OPS = 1000


# ------------------------------------------------------------------
# Small helper functions
# ------------------------------------------------------------------
def timed(label, fn):
    """Run fn(), print how long it took in milliseconds, and return that time.

    fn is a function with no arguments (usually a lambda), so we can pass
    "the work to do" into timed() without running it first.
    """
    start = time.perf_counter()               # high-precision clock
    fn()                                      # do the actual work
    ms = (time.perf_counter() - start) * 1000  # seconds -> milliseconds
    print(f"   {label:<34}{ms:>10.1f} ms")   # pad label to 34 chars, time to 10
    return ms


def section(title):
    """Print a section header between ==== lines."""
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


# ==================================================================
# STEP 1: Build the SAME 'orders' table in both databases
# ==================================================================
# A wide table (15 columns), like a real e-commerce orders table.
# Width matters: analytical queries usually need only 2-3 columns,
# and a column store can skip the other 12. A row store can't.
#
# GEN_SQL creates fake data inside DuckDB itself, which is much faster
# than generating it in a Python loop:
#   - range(N) produces numbers 0..N-1, one row each, named i
#   - random() gives a random number in [0, 1)
#   - ['North','South',...][1 + (i % 5)] cycles through a list
#     (DuckDB lists start at index 1, not 0)
#   - ::INT and ::VARCHAR are type casts
GEN_SQL = f"""
SELECT
  i                                   AS order_id,
  (random()*1000000)::INT             AS customer_id,
  (random()*50000)::INT               AS product_id,
  ['North','South','East','West','Central'][1 + (i % 5)] AS region,
  ['Electronics','Books','Fashion','Home','Toys','Sports'][1 + (i % 6)] AS category,
  round(random()*5000, 2)             AS amount,
  1 + (random()*5)::INT               AS quantity,
  (DATE '2024-01-01' + (i % 730)::INT)::VARCHAR AS order_date,
  ['PLACED','SHIPPED','DELIVERED','RETURNED'][1 + (i % 4)] AS status,
  ['UPI','CARD','COD','WALLET'][1 + (i % 4)] AS payment_method,
  'Some street address line for order ' || i AS shipping_address,
  md5(i::VARCHAR)                     AS tracking_id,
  (random()*100)::INT                 AS discount_pct,
  ['web','android','ios'][1 + (i % 3)] AS channel,
  'Customer note / gift message ' || (i % 997) AS notes
FROM range({N}) t(i)
"""

# Table schema, shared by both databases.
# order_id is the PRIMARY KEY: SQLite uses it as the key of its B-tree,
# which is what makes point lookups by order_id so fast there.
COLS = ("order_id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER, "
        "region TEXT, category TEXT, amount REAL, quantity INTEGER, order_date TEXT, "
        "status TEXT, payment_method TEXT, shipping_address TEXT, tracking_id TEXT, "
        "discount_pct INTEGER, channel TEXT, notes TEXT")

# Delete files left over from a previous run so every run starts clean.
# Without this, CREATE TABLE fails with "table orders already exists".
#
# Besides the main database file, both databases may leave helper files
# for crash safety. They are normally removed on a clean close, but can be
# left behind if a run crashed or was stopped with Ctrl+C:
#   orders_col.duckdb.wal      DuckDB's write-ahead log
#   orders_row.sqlite-wal      SQLite's write-ahead log (we turn WAL mode on below)
#   orders_row.sqlite-shm      SQLite's shared-memory index for the WAL file
#   orders_row.sqlite-journal  SQLite's rollback journal (used when WAL mode is off)
# A stale helper file next to a NEW database file could be replayed into it,
# so we delete them together with the main files.
old_files = [
    SQLITE_FILE,
    SQLITE_FILE + "-wal",
    SQLITE_FILE + "-shm",
    SQLITE_FILE + "-journal",
    DUCK_FILE,
    DUCK_FILE + ".wal",
]
for f in old_files:
    if os.path.exists(f):
        os.remove(f)

section(f"SETUP: loading {N:,} rows into both databases")   # {N:,} -> 5,000,000

# ---- Load DuckDB ----
t = time.perf_counter()
duck = duckdb.connect(DUCK_FILE)    # opens the file, creating it if needed
# DuckDB calls floating-point numbers DOUBLE instead of REAL, so swap the type name.
duck.execute(f"CREATE TABLE orders ({COLS.replace('REAL', 'DOUBLE')})")
duck.execute(f"INSERT INTO orders {GEN_SQL}")   # generate + insert N rows in one bulk statement
duck.execute("CHECKPOINT")   # flush everything from the write-ahead log into the main file
print(f"   DuckDB loaded in {time.perf_counter() - t:.1f}s")

# ---- Load SQLite (copying the exact same rows out of DuckDB) ----
t = time.perf_counter()
# isolation_level=None -> autocommit mode: each statement is its own transaction,
# like a typical web app (one request = one small write). This matters for the
# OLTP INSERT/UPDATE tests later.
lite = sqlite3.connect(SQLITE_FILE, isolation_level=None)
# WAL + synchronous=NORMAL are the common production settings for SQLite apps:
# fast commits that are still safe against crashes.
lite.execute("PRAGMA journal_mode=WAL")
lite.execute("PRAGMA synchronous=NORMAL")
lite.execute(f"CREATE TABLE orders ({COLS})")

# For the initial bulk load, wrap all inserts in ONE transaction.
# Otherwise every one of the millions of rows would be committed separately,
# and loading would take far longer.
lite.execute("BEGIN")
cur = duck.execute("SELECT * FROM orders ORDER BY order_id")
# Read from DuckDB in chunks of 100K rows so we never hold all N rows in memory.
# The := ("walrus") operator assigns and tests in one go: loop until fetchmany() returns [].
while batch := cur.fetchmany(100_000):
    # 15 '?' placeholders, one per column; executemany inserts the whole batch.
    lite.executemany(f"INSERT INTO orders VALUES ({','.join('?' * 15)})", batch)
lite.execute("COMMIT")
print(f"   SQLite loaded in {time.perf_counter() - t:.1f}s")

# Warm-up: read the whole table once in each database so the OS has the files
# cached in memory. The timings below then compare the ENGINES,
# not how fast the disk is on the first read.
lite.execute("SELECT count(*), sum(amount) FROM orders").fetchall()
duck.execute("SELECT count(*), sum(amount) FROM orders").fetchall()

# Collect (type, test name, sqlite_ms, duckdb_ms) for the final summary table.
results = []


# ==================================================================
# STEP 2: OLTP = tiny, frequent operations, each touching ONE row
# ==================================================================
# Think: "show me order #123", "place a new order", "mark order #456 shipped".
section(f"OLTP  (each test = {OLTP_OPS} separate single-row operations)")

# The same 1000 random order_ids are used for BOTH databases, so they do identical work.
ids = [random.randrange(N) for _ in range(OLTP_OPS)]

# Each function below takes a connection (SQLite or DuckDB) and RETURNS a
# function that does the work. Why the extra layer? Because timed() needs a
# function it can call later. Both sqlite3 and duckdb support conn.execute(sql, params),
# so the same code works for both databases.


def point_lookup(conn):
    # 1000 separate queries, each fetching one full row by primary key.
    # Row store: walk down the B-tree to one page; the whole row is there together.
    # Column store: the row's 15 values live in 15 different column segments.
    return lambda: [conn.execute("SELECT * FROM orders WHERE order_id = ?", [i]).fetchall()
                    for i in ids]


def single_inserts(conn, start):
    # 1000 separate INSERT statements, each committed on its own (autocommit).
    # 'start' is the first new order_id, so we don't collide with existing ones.
    def run():
        for k in range(OLTP_OPS):
            conn.execute(
                f"INSERT INTO orders VALUES ({','.join('?' * 15)})",
                [start + k, 1, 1, 'North', 'Books', 99.0, 1, '2025-01-01', 'PLACED',
                 'UPI', 'addr', 'trk', 0, 'web', 'note'])
    return run


def single_updates(conn):
    # 1000 separate UPDATEs, each changing one column of one row.
    return lambda: [conn.execute("UPDATE orders SET status = 'SHIPPED' WHERE order_id = ?", [i])
                    for i in ids]


# Each entry: (test name, the SQLite version of the work, the DuckDB version of the work).
# N + 1 is the first unused order_id for the inserts. Both databases get the
# same ids because their tables are separate.
for name, s_fn, d_fn in [
    ("Point lookup by order_id", point_lookup(lite), point_lookup(duck)),
    ("Single-row INSERTs", single_inserts(lite, N + 1), single_inserts(duck, N + 1)),
    ("Single-row UPDATEs", single_updates(lite), single_updates(duck)),
]:
    print(f"\n-- {name}")
    s = timed("SQLite (row store)", s_fn)
    d = timed("DuckDB (column store)", d_fn)
    results.append(("OLTP", name, s, d))


# ==================================================================
# STEP 3: OLAP = few big queries that scan ALL rows
#          but read only 2-3 of the 15 columns
# ==================================================================
# Think: dashboards and reports. "Revenue per region this year."
# Row store: must read every full row (all 15 columns) just to get 2 of them.
# Column store: reads only the 'region' and 'amount' columns, which are
# compressed and processed in batches (vectorized), not one row at a time.
section("OLAP  (scan millions of rows, read only 2-3 of 15 columns)")

OLAP = {
    "Revenue by region":
        "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC",
    "Avg order value by category+status":
        "SELECT category, status, AVG(amount), COUNT(*) FROM orders GROUP BY 1,2",
    "Monthly revenue trend":
        # substr(order_date, 1, 7) turns '2024-03-15' into '2024-03'
        "SELECT substr(order_date,1,7) AS month, SUM(amount * quantity) FROM orders GROUP BY 1 ORDER BY 1",
}

# The exact same SQL text runs on both databases.
# fetchall() matters: it forces the query to fully finish and return its results.
for name, sql in OLAP.items():
    print(f"\n-- {name}\n   {sql}")
    s = timed("SQLite (row store)", lambda: lite.execute(sql).fetchall())
    d = timed("DuckDB (column store)", lambda: duck.execute(sql).fetchall())
    results.append(("OLAP", name, s, d))


# ==================================================================
# STEP 4: Fairness check: "Isn't DuckDB just using more CPU cores?"
# ==================================================================
# SQLite always runs a query on one thread. DuckDB uses all cores by default.
# Forcing DuckDB to 1 thread shows the win comes mostly from columnar storage
# + vectorized execution, not from parallelism.
section("FAIRNESS CHECK: DuckDB on a single thread")
duck.execute("SET threads = 1")
sql = OLAP["Revenue by region"]
s = timed("SQLite (1 thread, always)", lambda: lite.execute(sql).fetchall())
d = timed("DuckDB (forced to 1 thread)", lambda: duck.execute(sql).fetchall())
duck.execute("RESET threads")   # back to the default (all cores)


# ==================================================================
# STEP 5: Column pruning: query cost depends on how many columns you touch
# ==================================================================
# Column store: reading 1 column vs 15 columns is a big difference
# (it really reads 15x more data).
# Caveat: to "touch" all 15 columns we glue them into one string, and that
# string building adds cost in BOTH databases. Treat these numbers as
# directional, not exact.
section("COLUMN PRUNING: read 1 column vs all 15 columns")
one = "SELECT SUM(amount) FROM orders"
# DuckDB version: concat_ws('|', a, b, c, ...) joins values with '|'.
allc = ("SELECT SUM(length(concat_ws('|', order_id, customer_id, product_id, region, "
        "category, amount, quantity, order_date, status, payment_method, shipping_address, "
        "tracking_id, discount_pct, channel, notes))) FROM orders")
# SQLite has no concat_ws, so turn the same query into (a || '|' || b || '|' || c ...).
allc_lite = allc.replace("concat_ws('|', ", "(").replace(", ", " || '|' || ")
timed("SQLite  1 column", lambda: lite.execute(one).fetchall())
timed("SQLite 15 columns", lambda: lite.execute(allc_lite).fetchall())
timed("DuckDB  1 column", lambda: duck.execute(one).fetchall())
timed("DuckDB 15 columns", lambda: duck.execute(allc).fetchall())


# ==================================================================
# STEP 6: Storage size on disk (same data in both)
# ==================================================================
# Values within one column look alike (e.g. 'North','South' repeating),
# so a column store compresses them very well. A row store mixes
# different types side by side and compresses poorly.
section("STORAGE ON DISK (same data)")
lite.close(); duck.close()   # close first so all data is flushed into the files
print(f"   SQLite: {os.path.getsize(SQLITE_FILE) / 1e6:8.1f} MB")   # bytes -> MB
print(f"   DuckDB: {os.path.getsize(DUCK_FILE) / 1e6:8.1f} MB")


# ==================================================================
# Summary table: the slide-worthy result
# ==================================================================
section("SUMMARY")
print(f"   {'Type':<6}{'Test':<36}{'SQLite':>10}{'DuckDB':>10}   Winner")
for kind, name, s, d in results:
    # Whoever is faster wins; "Nx" = how many times faster the winner was.
    win = f"SQLite {d / s:.0f}x" if s < d else f"DuckDB {s / d:.0f}x"
    print(f"   {kind:<6}{name:<36}{s:>8.0f}ms{d:>8.0f}ms   {win}")
