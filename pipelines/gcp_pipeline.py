"""
GCP Cost Ingestion Pipeline

Pulls GCP billing data into PostgreSQL using BigQuery billing exports.

How it works:
1. GCP writes daily cost data into a BigQuery table automatically
2. The pipeline connects to BigQuery directly using Application Default Credentials
3. It queries only rows newer than the last run using export_time as a cursor
4. Rows are loaded into the gcp_costs table using append write disposition

Authentication: run 'gcloud auth application-default login' before running this pipeline.
"""

import os
import re
import sys
import dlt
from dlt.common import pendulum
from google.cloud import bigquery

# BigQuery doesn't support parameterized identifiers, so project/dataset/table
# are interpolated into the query string directly. Restrict them to the
# character set BigQuery identifiers actually allow before interpolating.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_identifier(name: str, label: str) -> str:
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid BigQuery {label}: {name!r}")
    return name


def bigquery_billing_table(
    table_name: str,
    dataset: str = None,
    project_id: str = None,
    initial_start_date: str = None,
):
    table_name = _validate_identifier(table_name, "table name")
    dataset = _validate_identifier(dataset, "dataset")
    project_id = _validate_identifier(project_id, "project id")

    if initial_start_date:
        initial_value = pendulum.parse(initial_start_date)
    else:
        initial_value = pendulum.parse("2000-01-01T00:00:00Z")

    @dlt.resource(
        write_disposition="merge",
        primary_key=["usage_start_time", "project_id", "service_id", "sku_id"],
        columns={
            # GCP legitimately emits null project/service/sku on tax, credit,
            # and adjustment lines, so these merge-key columns can't be NOT NULL.
            "project_id": {"nullable": True},
            "service_id": {"nullable": True},
            "sku_id": {"nullable": True},
        },
    )
    def _load_table(
        incremental: dlt.sources.incremental[str] = dlt.sources.incremental("export_time", initial_value=initial_value)
    ):
        client = bigquery.Client(project=project_id)

        last_value = incremental.last_value

        query = f"""
            SELECT * FROM `{project_id}.{dataset}.{table_name}`
            WHERE export_time > @last_value
            ORDER BY export_time
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("last_value", "TIMESTAMP", last_value)
            ]
        )

        # GCP's detailed billing export re-emits rows (with a later export_time)
        # for usage periods whose cost is finalized/adjusted after the initial
        # export. The primary key deliberately excludes `cost` so a correction
        # overwrites the prior row via merge instead of appending a duplicate.
        print(f'Loading {table_name} (incremental from {last_value})...')
        for row in client.query(query, job_config=job_config):
            record = dict(row.items())
            project = record.get("project") or {}
            service = record.get("service") or {}
            sku = record.get("sku") or {}
            record["project_id"] = project.get("id")
            record["service_id"] = service.get("id")
            record["service_description"] = service.get("description")
            record["sku_id"] = sku.get("id")
            yield record

    return _load_table.with_name("bigquery_billing_table")


def load_standalone_table_resource() -> None:
    destination = os.getenv("DLT_DESTINATION", "filesystem")

    try:
        pipeline_name = dlt.config["pipeline.pipeline_name"]
    except KeyError:
        pipeline_name = "gcp_cost_pipeline"

    try:
        dataset_name = dlt.config["sources.gcp_billing.dataset_name"]
    except KeyError:
        dataset_name = "gcp_costs"

    try:
        initial_start_date = dlt.config["sources.gcp_billing.initial_start_date"]
    except KeyError:
        initial_start_date = None

    project_id = dlt.config["sources.gcp_billing.project_id"]
    dataset = dlt.config["sources.gcp_billing.dataset"]
    table_names = dlt.config["sources.gcp_billing.table_names"]

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    resources = [bigquery_billing_table(t, dataset=dataset, project_id=project_id, initial_start_date=initial_start_date) for t in table_names]

    info = pipeline.run(resources, loader_file_format="parquet")

    print(f"Pipeline {pipeline.pipeline_name} completed successfully")
    print(f"Loaded {len(table_names)} tables to {pipeline.destination}")


if __name__ == "__main__":
    try:
        load_standalone_table_resource()
        print("done")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
