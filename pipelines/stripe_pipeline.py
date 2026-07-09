"""
Stripe Revenue Pipeline

Pulls Stripe balance transactions into PostgreSQL using the Stripe REST API.
"""

import os
import dlt
import requests


def get_balance_transactions(api_key, start_date, end_date):
    url = "https://api.stripe.com/v1/balance_transactions"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "limit": 100,
        "created[gte]": start_date,
        "created[lte]": end_date
    }

    while True:
        response = requests.get(url, headers=headers, params=params).json()

        for transaction in response["data"]:
            yield transaction

        if not response["has_more"]:
            break

        params["starting_after"] = response["data"][-1]["id"]


@dlt.resource(name="stripe_revenue", write_disposition="merge", primary_key="id")
def stripe_revenue():
    api_key = dlt.secrets["sources.stripe.api_key"]
    start_date = dlt.config["sources.stripe.start_date"]
    end_date = dlt.config["sources.stripe.end_date"]

    for transaction in get_balance_transactions(api_key, start_date, end_date):
        yield transaction


if __name__ == "__main__":
    try:
        pipeline = dlt.pipeline(
            pipeline_name="stripe_revenue_pipeline",
            destination="postgres",
            dataset_name="stripe_revenue"
        )

        load_info = pipeline.run(stripe_revenue())
        print("done")
    except Exception as e:
        print(f"Pipeline failed: {e}")
