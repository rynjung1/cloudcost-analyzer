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

import csv
import hashlib
import io
import os
import sys
import time
import dlt
from azure.identity import ClientSecretCredential

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from http_client import session_with_retries

http = session_with_retries()

MAX_POLL_ATTEMPTS = 60  # 60 * 5s = 5 minutes


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

    response = http.post(url, headers=headers, json=body)

    if response.status_code != 202:
        raise Exception(f"Failed to generate report: {response.status_code} {response.text}")

    return response.headers["Location"]


def poll_until_ready(token, polling_url):
    headers = {"Authorization": f"Bearer {token}"}

    for _ in range(MAX_POLL_ATTEMPTS):
        response = http.get(polling_url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 202:
            print("still generating...")
            time.sleep(5)
        else:
            raise Exception(f"Unexpected status: {response.status_code}")

    raise TimeoutError(f"Report was not ready after {MAX_POLL_ATTEMPTS} polling attempts")


def download_csv(result):
    download_url = result["manifest"]["blobs"][0]["blobLink"]
    response = http.get(download_url)

    if response.status_code != 200:
        raise Exception(f"Failed to download cost report blob: {response.status_code} {response.text}")

    return response.text


@dlt.resource(name="azure_costs", write_disposition="merge", primary_key="resource_id")
def azure_costs(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        if not row.get("ResourceId"):
            # Charges with no ResourceId (tax, credits, marketplace adjustments)
            # would otherwise all merge onto the same empty-string primary key
            # and silently overwrite each other. Derive a stable synthetic key
            # from the rest of the row so distinct charges aren't collapsed,
            # while re-ingesting the same row stays idempotent.
            digest = hashlib.sha256(
                "|".join(f"{k}={v}" for k, v in sorted(row.items())).encode("utf-8")
            ).hexdigest()
            row["ResourceId"] = f"unassigned:{digest}"
        yield row


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

        load_info = pipeline.run(azure_costs(csv_text))
        print("done")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
