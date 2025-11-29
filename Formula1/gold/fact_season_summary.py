from pyspark.sql.functions import sum, countDistinct, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

season_summary_df = race_results_df.groupBy("race_year").agg(
    countDistinct("race_id").alias("total_races"),
    countDistinct("driver_id").alias("total_drivers"),
    countDistinct("constructor_id").alias("total_constructors"),
    sum("points").alias("total_points_awarded")
).withColumn("created_date", current_timestamp())

merge_condition = "tgt.race_year = src.race_year"

merge_delta_data(
    season_summary_df,
    "f1_presentation.fact_season_summary",
    "/mnt/formula1/presentation/fact_season_summary",
    merge_condition
)
