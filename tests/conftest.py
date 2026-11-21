"""Builds the whole platform once into a temp folder (day 1 + incremental day 2), then the
tests assert on the result. Takes ~1-2 minutes because it is a real Spark run."""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pytest

os.environ["TZ"] = "UTC"  # Python <-> Spark timestamp conversion uses the local zone
if hasattr(time, "tzset"):
    time.tzset()

ROOT = Path(__file__).resolve().parents[1]
_TMP = Path(tempfile.mkdtemp(prefix="tlake-test-"))
os.environ["TLAKE_LAKE"] = str(_TMP / "lakehouse")
os.environ["TLAKE_SOURCES"] = str(_TMP / "sources")
os.environ["TLAKE_EXPORTS"] = str(_TMP / "exports")
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def platform():
    from tlake import audience, generate, pipeline
    from tlake.rag import ingest

    generate.generate(1)
    day1 = pipeline.run()
    generate.generate(2)
    day2 = pipeline.run()
    aud = audience.build(ROOT / "config/campaigns/sam_caribbean_cruise.yaml")
    idx = ingest.build_index()
    yield {"day1": day1, "day2": day2, "audience": aud, "index": idx, "tmp": _TMP}
    shutil.rmtree(_TMP, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(platform):
    from tlake import lake
    return lake.spark()
