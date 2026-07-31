import os
import random

# Fixed seed + a large enough row count that the assert's 50%-sample null
# rate reliably lands well under the 6% max in blueprint.yml — 1,000 rows
# gave the sample enough variance to legitimately flip pass/fail between
# runs (empirically observed ~40% failure rate across repeated Spark
# samples of the old data). At 20,000 rows the sampling error is small
# enough that the observed rate stays in a ~4.3%-5.2% band across hundreds
# of trials, so the demo is deterministic in practice while still showing
# a real (not trivial) sampling-based quality gate.
SEED = 42
ROW_COUNT = 20_000

def main():
    print(f"Generating {ROW_COUNT:,} records with controlled NULLs (seed={SEED})...")
    random.seed(SEED)
    header = "id,name,email"
    rows = [header]

    for i in range(1, ROW_COUNT + 1):
        name = f"User {i}"
        # ~5% null rate for email
        email = f"user{i}@example.com" if random.random() > 0.05 else ""
        rows.append(f"{i},{name},{email}")

    os.makedirs("data/input", exist_ok=True)
    with open("data/input/users.csv", "w") as f:
        f.write("\n".join(rows) + "\n")

    print(f"✓ Created data/input/users.csv (with ~5% NULL emails, seed={SEED}).")

if __name__ == "__main__":
    main()
