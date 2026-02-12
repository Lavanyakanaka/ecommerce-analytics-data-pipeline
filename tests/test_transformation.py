import pandas as pd

from scripts.transformation import staging_to_production


def test_cleanse_customer_data_removes_nulls():
	df = pd.DataFrame(
		[
			{"customer_id": 1, "first_name": "A", "last_name": "B", "email": "a@b.com"},
			{"customer_id": 2, "first_name": None, "last_name": "C", "email": "c@d.com"},
		]
	)
	cleaned = staging_to_production.cleanse_customer_data(df)
	assert cleaned["customer_id"].tolist() == [1]


def test_apply_business_rules_filters_invalid_items():
	df = pd.DataFrame(
		[
			{"quantity": 2, "unit_price": 10},
			{"quantity": 0, "unit_price": 5},
		]
	)
	filtered = staging_to_production.apply_business_rules(df, "items")
	assert len(filtered) == 1
