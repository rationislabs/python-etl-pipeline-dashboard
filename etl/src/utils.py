def setup_bigquery_credentials():
    """Set up BigQuery credentials."""
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials.json"