# python-etl-pipeline-dashboard
ETL pipeline in python containerized in docker and then data is visualized using a dashboard of your choice
Example dashboards include: Ploty dash, nicegui, streamlit, holoviz panel, bokeh, redash, and reflex (more of a website building tool)

# Traffic Data Project
All-in-one Data Engineering project: ETL pipeline with Apache Airflow, Python, Apache Spark, and Delta Lakes for NYC Taxi traffic data, and Plotly Dash dashboard, containerized with Docker and deployed to Google Cloud (Cloud Composer, Cloud Run).

## Tools
- Python, pandas, pyspark, delta-spark, google-cloud-bigquery, google-cloud-storage
- Apache Airflow, Plotly Dash
- Docker, Invoke, GitHub Actions
- Google Cloud (Cloud Composer, Cloud Run, Cloud Storage)

## Setup (Local)
1. Install Docker Desktop[](https://www.docker.com/products/docker-desktop/) and Invoke (`pip install invoke`).
2. Install Google Cloud SDK[](https://cloud.google.com/sdk/docs/install).
3. Set up GCP credentials: `gcloud auth application-default login`, save as `credentials.json`.
4. Generate data: `python etl/src/generate_data.py` to create `data/traffic_data.csv`.
5. Unzip data (if needed): `unzip data/traffic_data.zip -d data/`
6. Build: `inv build`
7. Test: `inv test`
8. Deploy locally: `inv deploy-local` (Airflow: `http://localhost:8080`, Dashboard: `http://localhost:8050`)
9. Deploy to GCP: `inv deploy-cloud` (update tasks.py with your project ID)
10. Push to GitHub: `inv push`

## Outputs
- ETL processes NYC Taxi data into Delta Lake (parquet locally, Spark/Delta in cloud on Cloud Storage).
- Dashboard visualizes trips by borough (bar and pie charts).
- [Screenshots in output/screenshots/]

## Cloud Deployment
- **Google Cloud**:
  - Cloud Composer for Airflow (orchestrates ETL).
  - Cloud Run for containers (ETL and dashboard).
  - Cloud Storage for Delta Lake tables (gs://[YOUR_BUCKET]/delta_tables/).

## Notes
- Local ETL uses pandas for simplicity; cloud uses Spark for scalability and CV impact.
- Sample data and screenshots zipped for GitHub (<50 MB).