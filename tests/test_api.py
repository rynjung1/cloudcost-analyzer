import pytest
from fastapi.testclient import TestClient

import api.main as api_main

API_KEY = "test-api-key"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_main, "get_cached", lambda key: None)
    monkeypatch.setattr(api_main, "set_cached", lambda key, value: None)
    return TestClient(api_main.app)


def test_requires_api_key(client):
    response = client.get("/costs/summary")
    assert response.status_code == 401


def test_rejects_wrong_api_key(client):
    response = client.get("/costs/summary", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


def test_costs_summary_with_valid_key(client, monkeypatch):
    monkeypatch.setattr(
        api_main,
        "run_query",
        lambda query, params=None: [{"cloud_provider": "aws", "total_cost_usd": 42.0}],
    )

    response = client.get("/costs/summary", headers={"X-API-Key": API_KEY})

    assert response.status_code == 200
    assert response.json() == [{"cloud_provider": "aws", "total_cost_usd": 42.0}]


def test_unit_economics_handles_zero_revenue(client, monkeypatch):
    def fake_run_query(query, params=None):
        if "unified_cost_model" in query:
            return [{"total_cost_usd": 100.0}]
        return [{"total_revenue_usd": 0.0}]

    monkeypatch.setattr(api_main, "run_query", fake_run_query)

    response = client.get("/unit-economics", headers={"X-API-Key": API_KEY}, params={"month": "2024-01"})

    assert response.status_code == 200
    body = response.json()
    assert body["cost_as_pct_of_revenue"] is None
