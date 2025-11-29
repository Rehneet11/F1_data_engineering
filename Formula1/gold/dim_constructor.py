from pyspark.sql.functions import col, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

dim_constructor_df = race_results_df.select(
    "constructor_id",
    col("team").alias("constructor_name")
).dropDuplicates()\
.withColumn("created_date", current_timestamp())

merge_condition = "tgt.constructor_id = src.constructor_id"

merge_delta_data(
    dim_constructor_df,
    "f1_presentation.dim_constructor",
    "/mnt/formula1/presentation/dim_constructor",
    merge_condition
)
