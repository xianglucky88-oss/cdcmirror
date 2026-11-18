PY=python3
run:
	PYTHONPATH=src $(PY) -m cdcmirror.cli
verify:
	PYTHONPATH=src $(PY) -m cdcmirror.verify fixtures/verify/count_same_content_diff.json fixtures/verify/missing_delete.json fixtures/verify/shard_timeout.json
test:
	PYTHONPATH=src $(PY) -m pytest -q
clean:
	rm -rf .pytest_cache **/__pycache__ *.db
