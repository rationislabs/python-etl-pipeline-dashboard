import pandas as pd
import pytest
from etl import extract_data, transform_data

def test_extract_data():
    df = extract_data()
    assert not df.empty, "Dataframe is empty"
    assert set(["borough", "trip_count"]).issubset(df.columns), "Missing columns"

def test_transform_data():
    df = pd.DataFrame({"borough": ["Manhattan", "Brooklyn"], "trip_count": [100, 200]})
    df_transformed = transform_data(df)
    assert df_transformed['trip_count'].dtype == int, "Trip count not integer"