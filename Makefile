.PHONY: example fixture jar notebook test

VENV_PYTHON := .venv/bin/python

# Regenerates the synthetic fixture, rebuilds the connector jar, and re-executes the example
# notebook end-to-end — which also regenerates docs/assets/spark-asammdf/*.png, since the
# notebook writes those PNGs itself as part of running. Run this after any change to the
# connector, the Python helper, or the fixture generator.
example: fixture jar notebook

fixture:
	$(VENV_PYTHON) src/test/resources/generate_fixture.py

jar:
	sbt package

notebook:
	$(VENV_PYTHON) scripts/build_notebook.py
	$(VENV_PYTHON) -m jupyter nbconvert --to notebook --execute --inplace \
		notebooks/spark_asammdf_workflow.ipynb --ExecutePreprocessor.timeout=180

test:
	sbt test
	$(VENV_PYTHON) -m pytest src/test/python -v
