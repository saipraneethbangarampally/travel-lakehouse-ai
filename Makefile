export PYTHONPATH := src
PY ?= python

.PHONY: setup demo test lint mcp fabric-sql clean

setup:          ## install dependencies (needs Java 17+ for Spark)
	$(PY) -m pip install -r requirements.txt && $(PY) -m pip install -e .

demo:           ## day 1 + day 2 + audiences + RAG + MCP questions -> docs/demo_output.md
	$(PY) scripts/demo.py

test:           ## full end-to-end + unit test suite (~90s)
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src tests scripts

mcp:            ## start the MCP server on stdio
	$(PY) -m tlake.mcp_server

fabric-sql:     ## regenerate masking/RLS T-SQL from the semantic model
	$(PY) -m tlake.governance > docs/generated/fabric_security.sql

clean:
	rm -rf lakehouse sources exports
