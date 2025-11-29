from pyspark.sql.functions import col, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

dim_circuit_df = race_results_df.select(
    "circuit_id",
    col("circuit_location"),
    "race_id",
    "race_name",
    "race_year",
    "race_date"
).dropDuplicates()\
.withColumn("created_date", current_timestamp())

merge_condition = "tgt.circuit_id = src.circuit_id"

merge_delta_data(
    dim_circuit_df,
    "f1_presentation.dim_circuit",
    "/mnt/formula1/presentation/dim_circuit",
    merge_condition
)
