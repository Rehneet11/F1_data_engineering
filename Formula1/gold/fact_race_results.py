from pyspark.sql.functions import current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")\
    .withColumn("created_date", current_timestamp())

merge_condition = """
tgt.driver_id = src.driver_id AND
tgt.race_id = src.race_id
"""

merge_delta_data(
    race_results_df,
    "f1_presentation.fact_race_results",
    "/mnt/formula1/presentation/fact_race_results",
    merge_condition
)
