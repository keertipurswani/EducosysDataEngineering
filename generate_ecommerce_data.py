"""
Synthetic e-commerce data for the DuckDB hands-on
=================================================

Generates one year (2025) of orders for an Indian e-commerce store, shaped like
real data rather than uniform random noise:

  - Bigger cities place more orders (Bangalore, Mumbai, Delhi lead)
  - A few heavy buyers, a long tail of one-time buyers (Pareto), and some
    customers who signed up but never ordered
  - Premium members order more often and use cash-on-delivery less
  - Fixed product catalog with fixed list prices; cheap items sell more often
  - Seasonality: Republic Day, Independence Day and Diwali sales spike volume
    and discounts; weekends and evenings are busier
  - Payment mix: UPI dominates, COD for small orders, EMI for big ones
  - Status: mostly delivered; Fashion gets returned more; the last week of
    December is still in transit

Run it:
    pip install duckdb
    python generate_ecommerce_data.py                  # 50,000 orders, 8,000 customers
    python generate_ecommerce_data.py --orders 1000000 # bigger, for timing demos

Writes (same names the hands-on uses):
    data/orders.csv           one row per order
    data/orders.parquet       same data, typed and columnar
    data/customers.json       customer table (JSON array), joins on name = customer
    data/products.parquet     product catalog, joins on product
    data/monthly/orders_MM.csv  orders split by month, for 'data/monthly/*.csv' globs
"""

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

import duckdb

YEAR = 2025
OUT_DIR = "data"

# ------------------------------------------------------------------
# Reference data
# ------------------------------------------------------------------
# City -> relative share of customers (roughly by online-shopping volume)
CITIES = {
    "Bangalore": 18, "Mumbai": 17, "Delhi": 16, "Hyderabad": 10, "Chennai": 9,
    "Pune": 9, "Kolkata": 7, "Ahmedabad": 5, "Jaipur": 3, "Lucknow": 2,
    "Kochi": 2, "Chandigarh": 2,
}

FIRST_NAMES = [
    "Aarav", "Aditi", "Aditya", "Akash", "Amit", "Ananya", "Anil", "Anjali", "Ankit", "Arjun",
    "Asha", "Ayesha", "Bhavna", "Chetan", "Deepa", "Deepak", "Dev", "Divya", "Diya", "Farhan",
    "Gaurav", "Geeta", "Harish", "Ishaan", "Isha", "Jaya", "Karan", "Kavya", "Kiran", "Krishna",
    "Lakshmi", "Manish", "Meera", "Mohit", "Nandini", "Naveen", "Neha", "Nikhil", "Nisha", "Pooja",
    "Pranav", "Priya", "Rahul", "Rajesh", "Ravi", "Riya", "Rohan", "Sahil", "Sanjay", "Sara",
    "Shreya", "Siddharth", "Sneha", "Sunil", "Tanvi", "Tarun", "Uma", "Varun", "Vikram", "Zoya",
    "Abhishek", "Alok", "Bhavesh", "Christina", "Daniel", "Esha", "Gautam", "Hema", "Imran", "Jatin",
    "Kunal", "Lavanya", "Madhav", "Naina", "Omkar", "Pallavi", "Rakesh", "Ritika", "Sameer", "Swati",
    "Aisha", "Ajay", "Amrita", "Arnav", "Avni", "Chirag", "Dhruv", "Gayatri", "Harsh", "Ira",
    "Kabir", "Keerti", "Lalit", "Mahesh", "Mansi", "Mihir", "Nitin", "Parth", "Payal", "Prakash",
    "Radhika", "Rajiv", "Reema", "Rishi", "Ruchi", "Sakshi", "Shivani", "Shubham", "Sonal", "Suresh",
    "Tara", "Uday", "Vaishnavi", "Vinay", "Yash", "Zara", "Anita", "Ashok", "Bharat", "Kriti",
]
LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Iyer", "Reddy", "Nair", "Rao", "Patel", "Shah", "Mehta",
    "Singh", "Kumar", "Das", "Banerjee", "Chatterjee", "Mukherjee", "Joshi", "Kulkarni", "Desai", "Pillai",
    "Menon", "Agarwal", "Bansal", "Chopra", "Kapoor", "Malhotra", "Saxena", "Mishra", "Pandey", "Khan",
    "Sheikh", "Fernandes", "D'Souza", "Thomas", "Bose", "Ghosh", "Hegde", "Shetty", "Naidu", "Krishnan",
    "Bhat", "Jain", "Sinha", "Yadav", "Chauhan", "Rathore", "Arora", "Sethi", "Goel", "Mathur",
    "Acharya", "Bhatt", "Choudhary", "Dutta", "Gill", "Grover", "Kamath", "Kaur", "Khanna", "Lal",
    "Mahajan", "Modi", "Narayan", "Oberoi", "Prasad", "Raman", "Sahni", "Srinivasan", "Tiwari", "Venkatesh",
]
EMAIL_DOMAINS = {"gmail.com": 70, "yahoo.com": 10, "outlook.com": 10, "hotmail.com": 4, "rediffmail.com": 3, "icloud.com": 3}

# category -> [(product, list_price, popularity)]
CATALOG = {
    "Electronics": [
        ("Smartphone", 18999, 8), ("Laptop", 62999, 3), ("Wireless Earbuds", 2499, 12),
        ("Smartwatch", 4999, 6), ("Power Bank", 1299, 9), ("Bluetooth Speaker", 2999, 5),
        ("Mouse", 699, 8), ("Mechanical Keyboard", 3499, 3), ("Monitor", 11999, 2),
    ],
    "Fashion": [
        ("T-Shirt", 599, 14), ("Jeans", 1799, 9), ("Kurta", 1299, 8), ("Saree", 2999, 5),
        ("Sneakers", 3499, 6), ("Handbag", 1999, 4), ("Sunglasses", 999, 4),
    ],
    "Home & Kitchen": [
        ("Mixer Grinder", 3999, 3), ("Pressure Cooker", 2199, 4), ("Bedsheet Set", 1199, 6),
        ("Table Lamp", 1499, 3), ("Non-stick Pan", 1099, 5), ("Water Bottle", 449, 9),
    ],
    "Beauty": [
        ("Face Wash", 299, 10), ("Sunscreen", 549, 8), ("Perfume", 1799, 4), ("Hair Dryer", 1599, 2),
    ],
    "Grocery": [
        ("Basmati Rice 5kg", 649, 9), ("Dry Fruits Pack", 899, 6), ("Coffee Powder", 449, 6), ("Green Tea", 349, 5),
    ],
    "Books": [
        ("Fiction Novel", 399, 6), ("Programming Book", 899, 3), ("Exam Prep Guide", 549, 3),
    ],
    "Sports": [
        ("Yoga Mat", 799, 5), ("Dumbbell Set", 2499, 2), ("Cricket Bat", 2999, 2), ("Running Shoes", 3999, 3),
    ],
    "Furniture": [
        ("Office Chair", 8999, 2), ("Study Desk", 6999, 1), ("Bookshelf", 4499, 1), ("Sofa", 32999, 1),
    ],
}

# (start, end, volume multiplier, typical discounts) for sale events in YEAR
SALES = [
    (date(YEAR, 1, 17), date(YEAR, 1, 26), 2.0, [10, 15, 20, 30]),   # Republic Day sale
    (date(YEAR, 6, 1), date(YEAR, 6, 10), 1.6, [10, 20, 30, 40]),    # End of season sale
    (date(YEAR, 8, 8), date(YEAR, 8, 15), 2.0, [10, 15, 20, 30]),    # Independence Day sale
    (date(YEAR, 10, 1), date(YEAR, 10, 22), 3.0, [10, 20, 30, 40]),  # Festive / Diwali sale
    (date(YEAR, 11, 28), date(YEAR, 12, 1), 1.8, [10, 20, 30]),      # Black Friday weekend
]
NORMAL_DISCOUNTS = [0, 0, 0, 0, 5, 10]

# Orders per hour of day (IST): quiet at night, lunch bump, evening peak
HOUR_WEIGHTS = [2, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 7, 6, 6, 6, 7, 8, 10, 12, 12, 9, 5]


def sale_for(d):
    for start, end, mult, discounts in SALES:
        if start <= d <= end:
            return mult, discounts
    return None


def day_weight(d):
    w = 1.0
    if d.weekday() >= 5:
        w *= 1.25                      # weekends
    if d.month == 12:
        w *= 1.15                      # year-end gifting
    sale = sale_for(d)
    if sale:
        w *= sale[0]
    return w


def pick(weighted):
    """weighted: dict {value: weight}"""
    return random.choices(list(weighted), weights=list(weighted.values()))[0]


# ------------------------------------------------------------------
# Generators
# ------------------------------------------------------------------
def make_customers(n):
    combos = [(f, l) for f in FIRST_NAMES for l in LAST_NAMES]
    if n > len(combos):
        raise SystemExit(f"--customers can be at most {len(combos)} (unique names)")
    customers = []
    for i, (first, last) in enumerate(random.sample(combos, n), start=1):
        tier = "premium" if random.random() < 0.2 else "basic"
        handle = f"{first}.{last}".lower().replace("'", "")
        if random.random() < 0.5:
            handle += str(random.randint(1, 999))
        customers.append({
            "customer_id": i,
            "name": f"{first} {last}",
            "email": f"{handle}@{pick(EMAIL_DOMAINS)}",
            "city": pick(CITIES),
            "tier": tier,
            # how often this person shops: most buy rarely, a few buy a lot; premium ~2.5x more
            "activity": min(random.lognormvariate(0, 1.3), 30) * (2.5 if tier == "premium" else 1.0),
        })
    # ~8% sign up and never buy
    for c in random.sample(customers, int(n * 0.08)):
        c["activity"] = 0
    return customers


def make_orders(n, customers):
    days = [date(YEAR, 1, 1) + timedelta(days=i) for i in range((date(YEAR, 12, 31) - date(YEAR, 1, 1)).days + 1)]
    day_w = [day_weight(d) for d in days]
    products = [(cat, p, price, pop) for cat, items in CATALOG.items() for p, price, pop in items]
    prod_w = [pop for *_, pop in products]
    cust_w = [c["activity"] for c in customers]
    last_day = days[-1]

    chosen_days = random.choices(days, weights=day_w, k=n)
    chosen_prods = random.choices(products, weights=prod_w, k=n)
    chosen_custs = random.choices(customers, weights=cust_w, k=n)

    rows = []
    for d, (cat, product, price, _), c in zip(chosen_days, chosen_prods, chosen_custs):
        ts = datetime(d.year, d.month, d.day, random.choices(range(24), weights=HOUR_WEIGHTS)[0],
                      random.randint(0, 59), random.randint(0, 59))

        if price < 1000:
            qty = random.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        elif price < 5000:
            qty = random.choices([1, 2], weights=[90, 10])[0]
        else:
            qty = 1

        sale = sale_for(d)
        discount = random.choice(sale[1] if sale else NORMAL_DISCOUNTS)
        amount = round(price * qty * (1 - discount / 100))

        cod_w = 25 if amount < 2000 else 8
        if c["tier"] == "premium":
            cod_w //= 2
        payment = pick({"UPI": 45, "Credit Card": 18, "Debit Card": 10, "COD": cod_w,
                        "Net Banking": 4, "Wallet": 5, "EMI": 15 if amount >= 15000 else 0})

        if (last_day - d).days < 3:
            status = random.choices(["placed", "shipped"], weights=[60, 40])[0]
        elif (last_day - d).days < 7:
            status = random.choices(["shipped", "delivered", "cancelled"], weights=[55, 40, 5])[0]
        else:
            return_rate = 12 if cat == "Fashion" else 3
            cancel_rate = 9 if payment == "COD" else 4
            status = random.choices(["delivered", "returned", "cancelled"],
                                    weights=[100 - return_rate - cancel_rate, return_rate, cancel_rate])[0]

        rows.append([ts, c["name"], c["city"], cat, product, qty, price, discount, amount, payment, status])

    rows.sort(key=lambda r: r[0])                 # order ids follow time, like a real sequence
    out = []
    for i, r in enumerate(rows, start=1):
        ts = r[0]
        out.append([i, ts.date().isoformat(), ts.isoformat(sep=" ")] + r[1:])
    return out


def assign_signup_dates(customers, orders):
    first_order = {}
    for o in orders:
        first_order.setdefault(o[3], date.fromisoformat(o[1]))   # orders are time-sorted
    earliest = date(YEAR - 3, 1, 1)
    for c in customers:
        latest = first_order.get(c["name"], date(YEAR, 12, 31))
        c["signup_date"] = (earliest + timedelta(days=random.randint(0, (latest - earliest).days))).isoformat()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
ORDER_COLUMNS = ["order_id", "order_date", "ordered_at", "customer", "city", "category", "product",
                 "quantity", "unit_price", "discount_pct", "amount", "payment_method", "status"]
ORDER_TYPES = {"order_id": "INTEGER", "order_date": "DATE", "ordered_at": "TIMESTAMP", "customer": "VARCHAR",
               "city": "VARCHAR", "category": "VARCHAR", "product": "VARCHAR", "quantity": "INTEGER",
               "unit_price": "INTEGER", "discount_pct": "INTEGER", "amount": "INTEGER",
               "payment_method": "VARCHAR", "status": "VARCHAR"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--orders", type=int, default=50_000)
    ap.add_argument("--customers", type=int, default=8_000)
    ap.add_argument("--seed", type=int, default=42, help="same seed -> same data every run")
    args = ap.parse_args()
    random.seed(args.seed)

    os.makedirs(f"{OUT_DIR}/monthly", exist_ok=True)

    print(f"Generating {args.customers:,} customers and {args.orders:,} orders for {YEAR}...")
    customers = make_customers(args.customers)
    orders = make_orders(args.orders, customers)
    assign_signup_dates(customers, orders)

    # 1. orders.csv (written directly; everything else is derived from it by DuckDB)
    orders_csv = f"{OUT_DIR}/orders.csv"
    with open(orders_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(ORDER_COLUMNS)
        w.writerows(orders)

    con = duckdb.connect()
    con.sql(f"CREATE TABLE orders AS FROM read_csv('{orders_csv}', header = true, columns = {ORDER_TYPES})")

    # 2. orders.parquet
    con.sql(f"COPY orders TO '{OUT_DIR}/orders.parquet' (FORMAT parquet)")

    # 3. monthly CSVs
    for m in range(1, 13):
        con.sql(f"COPY (FROM orders WHERE month(order_date) = {m}) "
                f"TO '{OUT_DIR}/monthly/orders_{m:02d}.csv' (HEADER)")

    # 4. customers.json (JSON array)
    cust_rows = [(c["customer_id"], c["name"], c["email"], c["city"], c["tier"], c["signup_date"]) for c in customers]
    con.sql("CREATE TABLE customers (customer_id INTEGER, name VARCHAR, email VARCHAR, "
            "city VARCHAR, tier VARCHAR, signup_date DATE)")
    con.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)", cust_rows)
    con.sql(f"COPY customers TO '{OUT_DIR}/customers.json' (FORMAT json, ARRAY true)")

    # 5. products.parquet
    con.sql("CREATE TABLE products (product_id INTEGER, product VARCHAR, category VARCHAR, list_price INTEGER)")
    con.executemany("INSERT INTO products VALUES (?, ?, ?, ?)",
                    [(i, p, cat, price) for i, (cat, p, price, _) in
                     enumerate(((cat, p, price, pop) for cat, items in CATALOG.items() for p, price, pop in items), 1)])
    con.sql(f"COPY products TO '{OUT_DIR}/products.parquet' (FORMAT parquet)")

    # Summary
    print(f"\nWrote to ./{OUT_DIR}/: orders.csv, orders.parquet, customers.json, products.parquet, monthly/orders_01..12.csv\n")
    con.sql("SELECT order_id, ordered_at, customer, city, product, amount, status FROM orders LIMIT 5").show()
    con.sql("""
        SELECT month(order_date) AS m, monthname(order_date) AS month, count(*) AS orders, sum(amount) AS revenue
        FROM orders GROUP BY ALL ORDER BY m
    """).show(max_rows=12)


if __name__ == "__main__":
    main()
