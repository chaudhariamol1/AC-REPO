# Imports reccommended by databricks
from pyspark.sql.functions import lit, col, lag, when, rank, last, abs
from pyspark.sql import SparkSession, Window
from functools import lru_cache
import sys


@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder.getOrCreate())


spark = get_spark()


def spot_dt(data_frame, component_id):
    """
    Spot an oil change in loss factor and raise flags.
    
    Parameters
    ----------
    data_frame: PySpark DataFrame   
        Wide format dataframe to be flagged for
        
    sia_threshold: Double
        Threshold for what counts as sensor in air
        
    component_id: Integer
        Unique ID for this engine
    
    Returns
    -------
    data_frame: PySpark DataFrame   
        Wide format dataframe with alert and detect flags added
    """
    data_frame = data_frame.withColumn("DT", lit(0))
    
    return data_frame