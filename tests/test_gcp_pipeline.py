from datetime import datetime, timezone

import pytest

import pipelines.gcp_pipeline as gcp_pipeline
from pipelines.gcp_pipeline import _validate_identifier, bigquery_billing_table


def test_validate_identifier_accepts_normal_names():
    assert _validate_identifier("my-project_1", "project id") == "my-project_1"


@pytest.mark.parametrize("bad_value", ["", None, "proj`; DROP TABLE x; --", "a b", "a.b"])
def test_validate_identifier_rejects_unsafe_names(bad_value):
    with pytest.raises(ValueError):
        _validate_identifier(bad_value, "project id")


def test_bigquery_billing_table_rejects_unsafe_config():
    with pytest.raises(ValueError):
        bigquery_billing_table("costs; DROP TABLE x;", dataset="ds", project_id="proj")


def test_bigquery_billing_table_merges_on_dimensional_key_excluding_cost():
    resource = bigquery_billing_table("costs", dataset="ds", project_id="proj")
    hints = resource.compute_table_schema()

    assert hints["write_disposition"] == "merge"
    primary_key_columns = {name for name, col in hints["columns"].items() if col.get("primary_key")}
    assert primary_key_columns == {"usage_start_time", "project_id", "service_id", "sku_id"}
    assert "cost" not in primary_key_columns


def test_bigquery_billing_table_flattens_nested_dimensions(monkeypatch):
    raw_row = {
        "usage_start_time": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "export_time": datetime(2026, 6, 2, tzinfo=timezone.utc),
        "project": {"id": "proj-1", "name": "Project One"},
        "service": {"id": "svc-1", "description": "Compute Engine"},
        "sku": {"id": "sku-1", "description": "N1 instance"},
        "cost": 12.5,
    }

    class FakeRow:
        def items(self):
            return raw_row.items()

    class FakeQueryJob:
        def __iter__(self):
            return iter([FakeRow()])

    class FakeClient:
        def __init__(self, project=None):
            pass

        def query(self, query, job_config=None):
            return FakeQueryJob()

    monkeypatch.setattr(gcp_pipeline.bigquery, "Client", FakeClient)

    resource = bigquery_billing_table("costs", dataset="ds", project_id="proj")
    rows = list(resource)

    assert len(rows) == 1
    row = rows[0]
    assert row["project_id"] == "proj-1"
    assert row["service_id"] == "svc-1"
    assert row["service_description"] == "Compute Engine"
    assert row["sku_id"] == "sku-1"
    assert row["cost"] == 12.5
