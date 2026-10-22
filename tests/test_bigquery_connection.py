import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

project_id = os.getenv("GCP_PROJECT_ID")

if not project_id:
    raise ValueError("GCP_PROJECT_ID is missing from .env")

try:
    client = bigquery.Client(project=project_id)

    print("BigQuery authentication successful!")
    print("Project:", client.project)

    # List existing datasets without creating anything.
    datasets = list(client.list_datasets())

    print("Datasets found:", len(datasets))

    for dataset in datasets:
        print("-", dataset.dataset_id)

except Exception as error:
    print("BigQuery connection failed:")
    print(error)
    raise