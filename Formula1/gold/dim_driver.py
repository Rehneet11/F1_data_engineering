from pyspark.sql.functions import col, current_timestamp
from common_functions import merge_delta_data

race_results_df = spark.table("f1_presentation.race_results")

dim_driver_df = race_results_df.select(
    "driver_id",
    "driver_name",
    "driver_number",
    "driver_nationality"
).dropDuplicates()\
.withColumn("created_date", current_timestamp())

merge_condition = "tgt.driver_id = src.driver_id"

merge_delta_data(
    src_df=dim_driver_df,
    tgt_table="f1_presentation.dim_driver",
    tgt_path="/mnt/formula1/presentation/dim_driver",
    merge_condition=merge_condition
)
