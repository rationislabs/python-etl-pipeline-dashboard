from google.cloud import bigquery
import pandas as pd

def generate_data():
    client = bigquery.Client()
    query = """
    SELECT borough, COUNT(*) as trip_count
    FROM `bigquery-public-data.samples.nyc_taxi_trips`
    GROUP BY borough
    LIMIT 1000
    """
    df = client.query(query).to_dataframe()
    df.to_csv("data/traffic_data.csv", index=False)

if __name__ == "__main__":
    generate_data()