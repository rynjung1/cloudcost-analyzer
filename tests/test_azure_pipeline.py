import pytest

from pipelines.azure_pipeline import azure_costs, download_csv, poll_until_ready


def test_azure_costs_resource_merges_on_resource_id():
    hints = azure_costs.compute_table_schema()
    assert hints["write_disposition"] == "merge"
    assert hints["columns"]["resource_id"]["primary_key"] is True


def test_azure_costs_parses_csv_with_quoted_commas():
    csv_text = (
        'ResourceId,Date,MeterCategory,CostInUSD,Tags\n'
        '/sub/rg1,06/24/2026,Storage,12.50,"env=prod,team=data"\n'
        '/sub/rg2,06/25/2026,Compute,3.10,"env=dev"\n'
    )

    rows = list(azure_costs(csv_text))

    assert rows == [
        {
            "ResourceId": "/sub/rg1",
            "Date": "06/24/2026",
            "MeterCategory": "Storage",
            "CostInUSD": "12.50",
            "Tags": "env=prod,team=data",
        },
        {
            "ResourceId": "/sub/rg2",
            "Date": "06/25/2026",
            "MeterCategory": "Compute",
            "CostInUSD": "3.10",
            "Tags": "env=dev",
        },
    ]


def test_azure_costs_date_field_normalizes_to_usage_date_column():
    """Regression test for stg_azure_costs.sql's `date::date as usage_date`:
    dlt's snake_case naming convention must keep mapping Azure's real `Date`
    CSV field to a column literally named `date`."""
    from dlt.common.normalizers.naming.snake_case import NamingConvention

    assert NamingConvention().normalize_identifier("Date") == "date"


def test_poll_until_ready_times_out(monkeypatch):
    monkeypatch.setattr("pipelines.azure_pipeline.MAX_POLL_ATTEMPTS", 2)
    monkeypatch.setattr("pipelines.azure_pipeline.time.sleep", lambda seconds: None)

    class FakeResponse:
        status_code = 202

    monkeypatch.setattr("pipelines.azure_pipeline.http.get", lambda url, headers: FakeResponse())

    with pytest.raises(TimeoutError):
        poll_until_ready("token", "https://example.com/poll")


def test_poll_until_ready_returns_result_when_ready(monkeypatch):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"manifest": {"blobs": [{"blobLink": "https://example.com/data.csv"}]}}

    monkeypatch.setattr("pipelines.azure_pipeline.http.get", lambda url, headers: FakeResponse())

    result = poll_until_ready("token", "https://example.com/poll")

    assert result["manifest"]["blobs"][0]["blobLink"] == "https://example.com/data.csv"


def test_download_csv_returns_text_on_success(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = "ResourceId,CostInUSD\n/sub/rg1,1.00\n"

    monkeypatch.setattr("pipelines.azure_pipeline.http.get", lambda url: FakeResponse())

    result = {"manifest": {"blobs": [{"blobLink": "https://example.com/data.csv"}]}}
    assert download_csv(result) == "ResourceId,CostInUSD\n/sub/rg1,1.00\n"


def test_download_csv_raises_on_error_status(monkeypatch):
    class FakeResponse:
        status_code = 403
        text = "<Error>AuthenticationFailed</Error>"

    monkeypatch.setattr("pipelines.azure_pipeline.http.get", lambda url: FakeResponse())

    result = {"manifest": {"blobs": [{"blobLink": "https://example.com/data.csv"}]}}
    with pytest.raises(Exception, match="Failed to download cost report blob: 403"):
        download_csv(result)
