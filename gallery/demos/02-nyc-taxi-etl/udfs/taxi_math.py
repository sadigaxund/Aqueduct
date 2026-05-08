
def calc_duration_min(pickup_dt, dropoff_dt):
    """
    Core calculation logic. Handles nulls and returns 
    float duration in minutes.
    """
    if pickup_dt is None or dropoff_dt is None:
        return 0.0
    
    delta = dropoff_dt - pickup_dt
    return float(delta.total_seconds() / 60.0)
