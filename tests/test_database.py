from unittest.mock import MagicMock

import api.database as database


def test_run_query_executes_and_returns_rows(monkeypatch):
    fake_cursor = MagicMock()
    fake_cursor.__enter__.return_value = fake_cursor
    fake_cursor.__exit__.return_value = False
    fake_cursor.fetchall.return_value = [{"a": 1}]

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    monkeypatch.setattr(database, "get_connection", lambda: fake_conn)

    rows = database.run_query("select 1", ("param",))

    fake_cursor.execute.assert_called_once_with("select 1", ("param",))
    assert rows == [{"a": 1}]
    fake_conn.close.assert_called_once()


def test_run_query_closes_connection_even_on_error(monkeypatch):
    fake_cursor = MagicMock()
    fake_cursor.__enter__.return_value = fake_cursor
    fake_cursor.__exit__.return_value = False
    fake_cursor.execute.side_effect = RuntimeError("boom")

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    monkeypatch.setattr(database, "get_connection", lambda: fake_conn)

    try:
        database.run_query("select 1")
    except RuntimeError:
        pass

    fake_conn.close.assert_called_once()
