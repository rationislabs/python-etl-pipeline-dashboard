import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from google.cloud import bigquery
import os

def extract_data():
    """Extract data from BigQuery or local CSV."""
    if os.getenv("USE_BIGQUERY", "false") == "true":
        client = bigquery.Client()
        query = """
        SELECT borough, COUNT(*) as trip_count
        FROM `bigquery-public-data.samples.nyc_taxi_trips`
        GROUP BY borough
        LIMIT 5000
        """
        df = client.query(query).to_dataframe()
    else:
        df = pd.read_csv("data/traffic_data.csv")
    return df

def transform_data(df):
    """Transform data (e.g., clean, aggregate)."""
    df['trip_count'] = df['trip_count'].astype(int)
    return df

def load_data(df, output_path="output/delta_tables/traffic_delta", use_spark=False):
    """Load data to Delta Lake (pandas locally, Spark in cloud)."""
    if use_spark:
        builder = SparkSession.builder.appName("ETL") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/app/credentials.json")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").save(output_path)
        spark.stop()
    else:
        df.to_parquet(output_path, engine="pyarrow", partition_cols=["borough"])

def run_etl():
    """Run full ETL pipeline."""
    df = extract_data()
    df = transform_data(df)
    load_data(df, use_spark=os.getenv("USE_SPARK", "false") == "true")