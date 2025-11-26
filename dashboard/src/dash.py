
import dash
from dash import dcc, html
import plotly.express as px
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd
import os

app = dash.Dash(__name__)

# Load data (Delta Lake in cloud, parquet locally)
if os.getenv("USE_SPARK", "false") == "true":
    builder = SparkSession.builder.appName("Dashboard") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/app/credentials.json")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    df = spark.read.format("delta").load("gs://[YOUR_BUCKET]/delta_tables/traffic_delta").toPandas()
    spark.stop()
else:
    df = pd.read_parquet("output/delta_tables/traffic_delta")

# Layout
app.layout = html.Div([
    html.H1("NYC Taxi Traffic Dashboard"),
    dcc.Graph(figure=px.bar(df, x="borough", y="trip_count", title="Trips by Borough")),
    dcc.Graph(figure=px.pie(df, names="borough", values="trip_count", title="Trip Distribution"))
])

if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host='0.0.0.0')