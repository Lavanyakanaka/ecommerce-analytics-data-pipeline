import pandas as pd

from scripts.ingestion import ingest_to_staging


def test_bulk_insert_empty_returns_zero(monkeypatch):
	class DummyCursor:
		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

		def execute(self, *_args, **_kwargs):
			return None

	class DummyConn:
		def cursor(self):
			return DummyCursor()

		def commit(self):
			return None

	df = pd.DataFrame(columns=["col1", "col2"])
	assert ingest_to_staging.bulk_insert_data(df, "staging.test", DummyConn()) == 0


def test_load_csv_to_staging_uses_bulk_insert(monkeypatch, tmp_path):
	csv_path = tmp_path / "sample.csv"
	pd.DataFrame([{"a": 1}]).to_csv(csv_path, index=False)

	def fake_bulk_insert(df, table_name, connection):
		return len(df)

	monkeypatch.setattr(ingest_to_staging, "bulk_insert_data", fake_bulk_insert)
	result = ingest_to_staging.load_csv_to_staging(csv_path, "staging.sample", object())
	assert result["rows_loaded"] == 1
