def test_weather_etl_importable():
    import weather_etl

    assert weather_etl.__name__ == "weather_etl"
