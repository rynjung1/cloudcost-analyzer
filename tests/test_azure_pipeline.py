import pytest

from pipelines.azure_pipeline import azure_costs, poll_until_ready


def test_azure_costs_parses_csv_with_quoted_commas():
    csv_text = (
        'ResourceId,MeterCategory,CostInUSD,Tags\n'
        '/sub/rg1,Storage,12.50,"env=prod,team=data"\n'
        '/sub/rg2,Compute,3.10,"env=dev"\n'
    )

    rows = list(azure_costs(csv_text))

    assert rows == [
        {
            "ResourceId": "/sub/rg1",
            "MeterCategory": "Storage",
            "CostInUSD": "12.50",
            "Tags": "env=prod,team=data",
        },
        {
            "ResourceId": "/sub/rg2",
            "MeterCategory": "Compute",
            "CostInUSD": "3.10",
            "Tags": "env=dev",
        },
    ]


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
