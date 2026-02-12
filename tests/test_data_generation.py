from scripts.data_generation import generate_data


def test_generate_customers_columns():
	df = generate_data.generate_customers(5)
	assert {"customer_id", "first_name", "last_name", "email", "gender", "signup_date"}.issubset(
		set(df.columns)
	)


def test_generate_products_columns():
	df = generate_data.generate_products(3)
	assert {"product_id", "product_name", "category", "price"}.issubset(set(df.columns))


def test_generate_transactions_rows():
	customers = generate_data.generate_customers(2)
	df = generate_data.generate_transactions(4, customers)
	assert len(df) == 4
