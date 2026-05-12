import os
import random

def main():
    print("Generating 1,000 records with controlled NULLs...")
    header = "id,name,email"
    rows = [header]
    
    for i in range(1, 1001):
        name = f"User {i}"
        # ~5% null rate for email
        email = f"user{i}@example.com" if random.random() > 0.05 else ""
        rows.append(f"{i},{name},{email}")
        
    os.makedirs("data/input", exist_ok=True)
    with open("data/input/users.csv", "w") as f:
        f.write("\n".join(rows) + "\n")
    
    print("✓ Created data/input/users.csv (with ~5% NULL emails).")

if __name__ == "__main__":
    main()
