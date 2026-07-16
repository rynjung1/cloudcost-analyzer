import pytest

from pipelines.stripe_pipeline import get_balance_transactions, to_unix_timestamp


def test_to_unix_timestamp():
    assert to_unix_timestamp("1970-01-01") == 0
    assert to_unix_timestamp("2024-01-01") == 1704067200


def test_get_balance_transactions_paginates(monkeypatch):
    pages = [
        {"data": [{"id": "txn_1"}, {"id": "txn_2"}], "has_more": True},
        {"data": [{"id": "txn_3"}], "has_more": False},
    ]

    calls = []

    def fake_get(url, headers, params):
        calls.append(dict(params))

        class FakeResponse:
            status_code = 200

            def json(self):
                return pages[len(calls) - 1]

        return FakeResponse()

    monkeypatch.setattr("pipelines.stripe_pipeline.http.get", fake_get)

    transactions = list(get_balance_transactions("sk_test", "2024-01-01", "2024-01-02"))

    assert [t["id"] for t in transactions] == ["txn_1", "txn_2", "txn_3"]
    assert "starting_after" not in calls[0]
    assert calls[1]["starting_after"] == "txn_2"


def test_get_balance_transactions_raises_on_error_status(monkeypatch):
    class FakeResponse:
        status_code = 401
        text = '{"error": {"message": "Invalid API Key"}}'

        def json(self):
            return {"error": {"message": "Invalid API Key"}}

    monkeypatch.setattr("pipelines.stripe_pipeline.http.get", lambda url, headers, params: FakeResponse())

    with pytest.raises(Exception, match="Stripe API error: 401"):
        list(get_balance_transactions("sk_test", "2024-01-01", "2024-01-02"))
