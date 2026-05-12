import os
import random

def main():
    print("Generating products data with numeric prices...")
    header = "product_id,price"
    rows = [header]
    
    # Generate 1000 products with price around 100 (normal distribution)
    for i in range(1, 1001):
        price = round(random.normalvariate(100, 25), 2)
        price = max(0.01, price) # ensure positive
        rows.append(f"P{i},{price}")
    
    os.makedirs("data/input", exist_ok=True)
    with open("data/input/products.csv", "w") as f:
        f.write("\n".join(rows) + "\n")
    
    print("✓ Created data/input/products.csv")

if __name__ == "__main__":
    main()
