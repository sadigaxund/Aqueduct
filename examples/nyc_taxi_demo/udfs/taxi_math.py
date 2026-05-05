from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def _calc_duration_logic(pickup_dt, dropoff_dt):
    """
    Core calculation logic. Handles nulls and returns 
    float duration in minutes.
    """
    if pickup_dt is None or dropoff_dt is None:
        return 0.0
    
    delta = dropoff_dt - pickup_dt
    return float(delta.total_seconds() / 60.0)

# Define the UDF with an explicit Spark return type
# This is the 'entry' point referenced in the Blueprint
calc_duration_min = udf(_calc_duration_logic, DoubleType())
