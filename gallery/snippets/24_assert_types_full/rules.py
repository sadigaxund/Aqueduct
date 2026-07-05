from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_completed_max(df: DataFrame) -> dict:
    """Custom assert: completed orders must not exceed $1000.

    The callable receives a lazy DataFrame and returns a dict with
    passed/message/quarantine_df. It runs on the driver — the author
    owns the Spark action cost (here: one .count() and a filter).
    """
    bad = df.filter((col("status") == "completed") & (col("amount") > 1000))
    count = bad.count()
    passed = count == 0
    return {
        "passed": passed,
        "message": f"Found {count} completed order(s) with amount > $1000" if not passed else "",
        "quarantine_df": bad if not passed else None,
    }
