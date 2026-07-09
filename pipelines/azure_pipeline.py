"""
Azure Cost Ingestion Pipeline

Pulls Azure cost data into PostgreSQL using the Azure Cost Management API.

How the API works (async three-call pattern):
1. POST to generateCostDetailsReport - kicks off report generation for a given
   subscription and date range. Azure queues the job and returns 202 with a
   Location header (polling URL).
2. GET the Location URL repeatedly. 202 means still generating, 200 means done
   and the response body contains a manifest with blob download links.
3. GET the blob link to download the actual CSV cost data.
"""

import os
import time
import requests
import dlt
from azure.identity import ClientSecretCredential


def get_access_token():
    tenant_id = dlt.config["sources.azure_cur.tenant_id"]
    client_id = dlt.config["sources.azure_cur.client_id"]
    client_secret = dlt.config["sources.azure_cur.client_secret"]

    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return credential.get_token("https://management.azure.com/.default").token


def generate_cost_report(token, subscription_id, start_date, end_date):
    url = "https://management.azure.com/subscriptions/{}/providers/Microsoft.CostManagement/generateCostDetailsReport?api-version=2024-08-01".format(subscription_id)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "metric": "ActualCost",
        "timePeriod": {
            "start": start_date,
            "end": end_date
        }
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code != 202:
        raise Exception(f"Failed to generate report: {response.status_code} {response.text}")

    return response.headers["Location"]


def poll_until_ready(token, polling_url):
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        response = requests.get(polling_url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 202:
            print("still generating...")
            time.sleep(5)
        else:
            raise Exception(f"Unexpected status: {response.status_code}")


def download_csv(result):
    download_url = result["manifest"]["blobs"][0]["blobLink"]
    response = requests.get(download_url)
    return response.text


@dlt.resource(name="azure_costs", write_disposition="merge")
def azure_costs(csv_text):
    lines = csv_text.split("\n")
    headers = lines[0].split(",")

    for line in lines[1:]:
        if not line:
            continue
        values = line.split(",")
        yield dict(zip(headers, values))


if __name__ == "__main__":
    try:
        subscription_id = dlt.config["sources.azure_cur.subscription_id"]
        start_date = dlt.config["sources.azure_cur.start_date"]
        end_date = dlt.config["sources.azure_cur.end_date"]

        token = get_access_token()
        polling_url = generate_cost_report(token, subscription_id, start_date, end_date)
        result = poll_until_ready(token, polling_url)
        csv_text = download_csv(result)

        pipeline = dlt.pipeline(
            pipeline_name="azure_cost_pipeline",
            destination="postgres",
            dataset_name="azure_costs"
        )

        load_info = pipeline.run(resource=azure_costs(csv_text))
        print("done")
    except Exception as e:
        print(f"Pipeline failed: {e}")
