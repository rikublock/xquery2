test:
	pytest tests/

lint:
	pylint bench/ tests/ xquery/
