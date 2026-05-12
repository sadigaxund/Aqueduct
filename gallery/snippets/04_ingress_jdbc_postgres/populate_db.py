import os
import sqlite3
from pyspark.sql import SparkSession

def load_env():
    env = {}
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip().strip("'\"")
    return env

def main():
    print("--- Aqueduct JDBC Population Tool ---")
    env = load_env()
    
    jdbc_url = env.get("JDBC_URL", "jdbc:sqlite:data/input/inventory.db")
    user = env.get("JDBC_USER", "admin")
    password = env.get("JDBC_PASSWORD", "secret")

    # Auto-detect driver and jar package
    if "postgresql" in jdbc_url:
        driver = "org.postgresql.Driver"
        jars = "org.postgresql:postgresql:42.5.0"
    elif "sqlite" in jdbc_url:
        driver = "org.sqlite.JDBC"
        jars = "org.xerial:sqlite-jdbc:3.36.0.3"
    else:
        # Fallback to .env or generic
        driver = env.get("JDBC_DRIVER", "")
        jars = ""

    print(f"Target: {jdbc_url}")

    sample_data = [
        {"item_id": 1, "quantity": 150, "last_updated": "2024-01-01 10:00:00"},
        {"item_id": 2, "quantity": 0,   "last_updated": "2024-01-02 11:30:00"},
        {"item_id": 3, "quantity": 85,  "last_updated": "2024-01-03 09:15:00"},
        {"item_id": 4, "quantity": 300, "last_updated": "2024-01-04 14:00:00"}
    ]

    if "jdbc:sqlite:" in jdbc_url:
        # Use built-in sqlite3 for SQLite (Fastest, zero-dependency)
        db_path = jdbc_url.replace("jdbc:sqlite:", "")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS inventory")
        cursor.execute("CREATE TABLE inventory (item_id INTEGER PRIMARY KEY, quantity INTEGER, last_updated TEXT)")
        cursor.executemany("INSERT INTO inventory VALUES (?, ?, ?)", 
                           [(d["item_id"], d["quantity"], d["last_updated"]) for d in sample_data])
        conn.commit()
        conn.close()
        print(f"✓ Populated SQLite at {db_path}")
    else:
        # Use Spark for remote JDBC (Postgres, etc.)
        # This uses the same JDBC jars as Aqueduct
        print(f"Using Spark to populate {jdbc_url}...")
        
        # Apply cloudpickle patch for Python 3.14
        try:
            import cloudpickle
            import pyspark.cloudpickle as bundled
            bundled.dumps = cloudpickle.dumps
            bundled.loads = cloudpickle.loads
        except ImportError:
            pass
        
        spark = SparkSession.builder \
            .appName("PopulateJDBC") \
            .config("spark.jars.packages", jars) \
            .getOrCreate()
        
        df = spark.createDataFrame(sample_data)
        
        df.write.format("jdbc").options(
            url=jdbc_url,
            dbtable="inventory",
            user=user,
            password=password,
            driver=driver
        ).mode("overwrite").save()
        
        print(f"✓ Populated JDBC table 'inventory' at {jdbc_url}")
        spark.stop()

if __name__ == "__main__":
    main()
