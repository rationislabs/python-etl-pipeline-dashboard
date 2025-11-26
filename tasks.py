from invoke import task
import zipfile
import os

@task
def test(c):
    """Run pytests for ETL and dashboard."""
    c.run("pytest etl/tests")
    c.run("pytest dashboard/tests")

@task
def build(c):
    """Build Docker images for ETL and dashboard."""
    c.run("docker build -f etl/Dockerfile -t traffic-etl .")
    c.run("docker build -f dashboard/Dockerfile -t traffic-dashboard .")

@task
def deploy_local(c):
    """Deploy locally with Docker Compose."""
    c.run("docker-compose up -d")

@task
def deploy_cloud(c):
    """Deploy to GCP Cloud Run."""
    c.run("gcloud artifacts repositories create traffic-repo --repository-format=docker --location=us-central1 --description='Traffic Project Repo' || true")
    c.run("gcloud builds submit --tag us-central1-docker.pkg.dev/[YOUR_PROJECT]/traffic-repo/traffic-etl etl/")
    c.run("gcloud run deploy traffic-etl --image us-central1-docker.pkg.dev/[YOUR_PROJECT]/traffic-repo/traffic-etl --platform managed --region us-central1 --allow-unauthenticated")
    c.run("gcloud builds submit --tag us-central1-docker.pkg.dev/[YOUR_PROJECT]/traffic-repo/traffic-dashboard dashboard/")
    c.run("gcloud run deploy traffic-dashboard --image us-central1-docker.pkg.dev/[YOUR_PROJECT]/traffic-repo/traffic-dashboard --platform managed --region us-central1 --allow-unauthenticated --port 8050")
    # Upload DAG to Cloud Composer
    c.run("gsutil cp etl/src/airflow_dags/traffic_etl_dag.py gs://[YOUR_COMPOSER_BUCKET]/dags/")

@task
def zip_data(c):
    """Zip traffic data."""
    with zipfile.ZipFile("data/traffic_data.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("data/traffic_data.csv")

@task
def zip_output(c):
    """Zip dashboard screenshots."""
    with zipfile.ZipFile("output/screenshots.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for file in os.listdir("output/screenshots"):
            zf.write(f"output/screenshots/{file}")

@task
def push(c):
    """Push to GitHub."""
    c.run("git add .")
    c.run('git commit -m "Update project"')
    c.run("git push origin main")

@task
def all(c):
    """Run all: test, build, zip, deploy local, push."""
    test(c)
    build(c)
    zip_data(c)
    zip_output(c)
    deploy_local(c)
    push(c)