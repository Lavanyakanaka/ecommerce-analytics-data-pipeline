from scripts.quality_checks import validate_data


def test_calculate_quality_score():
	results = {
		"a": True,
		"b": True,
		"c": False,
		"d": True,
	}
	assert validate_data.calculate_quality_score(results) == 75.0
