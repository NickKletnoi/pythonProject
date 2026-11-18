"""Generate the mock Turso source data (seeded, reproducible). Run: python tools/make_data.py"""
import csv, random, datetime as dt, pathlib

random.seed(42)
OUT = pathlib.Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)

FIRST = ["Alice","Bob","Cara","Dan","Eve","Frank","Grace","Hector","Ines","Jamal","Kira","Liam","Mina","Noah","Olga",
         "Pavel","Quinn","Rosa","Sam","Tara","Uma","Vik","Wen","Xena","Yusuf","Zoe"]
LAST = ["Smith","Jones","Lee","Wu","Patel","Garcia","Kim","Nguyen","Brown","Silva","Khan","Meyer","Rossi","Cohen","Novak"]
COUNTRIES = ["US","CA","GB","DE","AU","IN","FR"]
SEGMENTS = ["retail","enterprise","smb"]
PRODUCTS = [("Widget","hardware",9.99),("Gadget","hardware",24.50),("Support Plan","services",100.00),
            ("Gizmo","hardware",5.00),("Cloud Seat","software",12.00),("Analytics Add-on","software",49.00),
            ("Starter Kit","accessories",19.99),("Pro Kit","accessories",59.99),("Cable Pack","accessories",7.50),
            ("Docking Station","hardware",129.00),("Monitor Stand","accessories",34.00),("Keyboard","hardware",45.00),
            ("Mouse","hardware",25.00),("Training Day","services",450.00),("Install Service","services",150.00),
            ("Backup Plan","software",15.00),("Security Suite","software",89.00),("Headset","hardware",79.00),
            ("Webcam","hardware",-1.00),            # 19: bad price in batch 1, corrected in batch 2
            ("Laptop Sleeve","accessories",29.00)]

def ts(day, hour=None):
    h = random.randint(7, 20) if hour is None else hour
    return f"{day.isoformat()} {h:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"

def rand_day(a, b):
    return a + dt.timedelta(days=random.randint(0, (b - a).days))

def write(name, header, rows):
    with open(OUT / name, "w", newline="") as f:
        w = csv.writer(f); w.writerow(header)
        for r in rows: w.writerow(["" if v is None else v for v in r])
    print(f"{name:28s} {len(rows):4d} rows")

C_HDR = ["customer_id","name","email","segment","country","updated_at"]
P_HDR = ["product_id","product_name","category","list_price","updated_at"]
S_HDR = ["order_line_id","order_id","customer_id","product_id","quantity","unit_price","order_ts","updated_at"]

def customer(cid, day):
    first, last = random.choice(FIRST), random.choice(LAST)
    name, email = f"{first} {last}", f"{first}.{last}{cid}@example.com".lower()
    seg, ctry = random.choice(SEGMENTS), random.choice(COUNTRIES)
    r = random.random()
    if r < 0.10: name = f"  {name} "                     # stray whitespace
    elif r < 0.18: email = email.upper()                 # shouting email
    elif r < 0.24: seg = seg.title()                     # inconsistent case
    elif r < 0.30: ctry = ctry.lower()                   # lower-case country
    elif r < 0.34: email = None                          # missing email
    return [cid, name, email, seg, ctry, ts(day)]

# ---------------- batch 1: initial load
B1 = (dt.date(2026,9,1), dt.date(2026,10,31))
cust1 = [customer(i, rand_day(*B1)) for i in range(1, 61)]
prod1 = [[i+1, f" {n} " if i % 7 == 3 else n, c.title() if i % 5 == 0 else c, p, ts(rand_day(*B1))]
         for i, (n, c, p) in enumerate(PRODUCTS)]
price = {r[0]: r[3] for r in prod1}

sales1, line, order = [], 10001, 5001
for _ in range(120):
    cust = random.randint(1, 60); day = rand_day(*B1)
    for _ in range(random.randint(1, 4)):
        pid = random.randint(1, 20); t = ts(day)
        up = round(abs(price[pid]) * random.choice([1, 1, 1, 0.9, 1.1]), 2)
        sales1.append([line, order, cust, pid, random.randint(1, 5), up, t, t]); line += 1
    order += 1
# deliberately bad rows (will be quarantined by the pipeline)
sales1[7][4] = 0                       # quantity 0
sales1[41][4] = -2                     # negative quantity
sales1[90][6] = None                   # missing order_ts
sales1[130][5] = -9.99                 # negative unit price
write("customers_batch1.csv", C_HDR, cust1)
write("products_batch1.csv", P_HDR, prod1)
write("sales_batch1.csv", S_HDR, sales1)

# ---------------- batch 2: changes, new rows, a late-arriving customer reference
B2 = (dt.date(2026,11,1), dt.date(2026,11,10))
random.seed(7)
cust2 = []
for cid in random.sample(range(1, 61), 8):                        # updates to existing customers
    r = customer(cid, rand_day(*B2)); r[3] = random.choice(SEGMENTS); cust2.append(r)
cust2 += [customer(i, rand_day(*B2)) for i in range(61, 67)]       # 6 new customers
prod2 = [[19, "Webcam", "hardware", 59.00, ts(rand_day(*B2))],     # fixes the bad price
         [1, "Widget", "hardware", 10.99, ts(rand_day(*B2))],      # price change
         [5, "Cloud Seat", "software", 13.50, ts(rand_day(*B2))],
         [21, "Smart Plug", "hardware", 14.00, ts(rand_day(*B2))],
         [22, "Premium Support", "services", 250.00, ts(rand_day(*B2))]]
price.update({r[0]: r[3] for r in prod2})
sales2 = []
for old in random.sample(sales1[:100], 12):                        # corrections to existing lines
    if old[4] in (0,) or old[6] is None: continue
    sales2.append([old[0], old[1], old[2], old[3], max(1, abs(int(old[4])) + 1), old[5], old[6], ts(rand_day(*B2))])
for _ in range(45):
    cust = random.choice(list(range(1, 67))); day = rand_day(*B2)
    for _ in range(random.randint(1, 3)):
        pid = random.choice(list(range(1, 23))); t = ts(day)
        sales2.append([line, order, cust, pid, random.randint(1, 5), round(abs(price[pid]), 2), t, t]); line += 1
    order += 1
for cust in (901, 902, 901):                                       # customers that do not exist yet
    t = ts(dt.date(2026,11,10)); pid = random.randint(1, 18)
    sales2.append([line, order, cust, pid, 2, round(price[pid], 2), t, t]); line += 1; order += 1
write("customers_batch2.csv", C_HDR, cust2)
write("products_batch2.csv", P_HDR, prod2)
write("sales_batch2.csv", S_HDR, sales2)

# ---------------- batch 3: the late customers finally arrive
random.seed(11)
cust3 = [customer(901, dt.date(2026,11,15)), customer(902, dt.date(2026,11,15))]
cust3[0][1], cust3[1][1] = "Late Arrival One", "Late Arrival Two"
write("customers_batch3.csv", C_HDR, cust3)
