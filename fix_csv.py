# fix_csv.py
import re, sys, csv

if len(sys.argv) < 3:
    print("uso: python fix_csv.py <in> <out>")
    sys.exit(1)

inp, outp = sys.argv[1], sys.argv[2]
with open(inp, "r", encoding="utf-8-sig") as f:
    lines = [ln.rstrip("\n") for ln in f if ln.strip()]

# header
if "," in lines[0]:
    header = [h.strip() for h in lines[0].split(",")]
    data = lines[1:]
else:
    header = ["date","product","price","quantity","customer","region","revenue"]
    data = lines

pat = re.compile(r"""
^\s*\d+\s+                          # idx
(\d{4}-\d{2}-\d{2})\s+              # date
([A-Za-z][\w-]*)\s+                 # product (1 palabra)
(\d+(?:\.\d+)?)\s+                  # price
(\d+)\s+                            # quantity
(\S+)\s+                            # customer
(\S+)\s+                            # region
(\d+(?:\.\d+)?)\s*$                 # revenue
""", re.X)

rows = []
for s in data:
    if "," in s:  # ya viene en csv real
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 7:
            rows.append(parts); continue
    m = pat.match(s.strip().strip("'").strip('"'))
    if m:
        rows.append(list(m.groups()))
        continue
    # fallback por split de espacios quitando índice
    parts = s.split()
    if parts and parts[0].isdigit() and len(parts) >= 8:
        rows.append(parts[1:8])

with open(outp, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(header)
    w.writerows(rows)
print(f"ok -> {outp} ({len(rows)} filas)")
