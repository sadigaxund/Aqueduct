from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import datetime

# 1. Setup Spark
spark = SparkSession.builder.appName("UDF_Test").getOrCreate()

# 2. Define UDF in your style
def logic(a, b):
    if a is None or b is None: return 0.0
    return float((b - a).total_seconds() / 60.0)

my_udf = udf(logic, DoubleType())

# 3. Test Registration
print("Testing registration...")
try:
    # This is what Aqueduct currently does (and fails)
    spark.udf.register("fail_test", my_udf, "double")
    print("✓ Registered with type (unexpected success)")
except Exception as e:
    print(f"✗ Failed with type as expected: {e}")

try:
    # This is the fix: pass None or omit the type if it's already a UDF object
    spark.udf.register("success_test", my_udf)
    print("✓ Registered successfully without redundant type!")
except Exception as e:
    print(f"✗ Failed without type: {e}")

# 4. Verify it works in SQL
df = spark.createDataFrame([
    (datetime.datetime(2024,1,1,12,0), datetime.datetime(2024,1,1,12,30))
], ["t1", "t2"])
df.createOrReplaceTempView("test_table")

spark.sql("SELECT success_test(t1, t2) as duration FROM test_table").show()

spark.stop()
