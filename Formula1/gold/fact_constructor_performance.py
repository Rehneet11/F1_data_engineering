from pyspark.sql.functions import sum, count, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

constructor_perf_df = race_results_df.groupBy(
    "race_year", "constructor_id", "team"
).agg(
    sum("points").alias("total_points"),
    count("*").alias("races_participated")
).withColumn("created_date", current_timestamp())

merge_condition = """
tgt.constructor_id = src.constructor_id AND
tgt.race_year = src.race_year
"""

merge_delta_data(
    constructor_perf_df,
    "f1_presentation.fact_constructor_performance",
    "/mnt/formula1/presentation/fact_constructor_performance",
    merge_condition
)
