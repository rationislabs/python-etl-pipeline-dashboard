import pandas as pd
import pytest
import os

def test_load_data():
    if os.getenv("USE_SPARK", "false") == "true":
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        builder = SparkSession.builder.appName("Test") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/app/credentials.json")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        df = spark.read.format("delta").load("gs://[YOUR_BUCKET]/delta_tables/traffic_delta").toPandas()
        spark.stop()
    else:
        df = pd.read_parquet("output/delta_tables/traffic_delta")
    assert not df.empty, "Dataframe is empty"
    assert set(["borough", "trip_count"]).issubset(df.columns), "Missing columns"