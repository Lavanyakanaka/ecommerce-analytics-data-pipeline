def transform(customers, products):
    customers["name"] = customers["name"].str.upper()
    products["price"] = products["price"].round(2)
    return customers, products
