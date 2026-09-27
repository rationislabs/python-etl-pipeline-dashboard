def test_weather_dash_importable():
    import weather_dash

    assert weather_dash.__name__ == "weather_dash"
