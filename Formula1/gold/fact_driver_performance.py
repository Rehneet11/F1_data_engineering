from pyspark.sql.functions import sum, count, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

driver_perf_df = race_results_df.groupBy(
    "race_year", "driver_id", "driver_name"
).agg(
    sum("points").alias("total_points"),
    count("*").alias("races_participated")
).withColumn("created_date", current_timestamp())

merge_condition = """
tgt.driver_id = src.driver_id AND
tgt.race_year = src.race_year
"""

merge_delta_data(
    driver_perf_df,
    "f1_presentation.fact_driver_performance",
    "/mnt/formula1/presentation/fact_driver_performance",
    merge_condition
)
