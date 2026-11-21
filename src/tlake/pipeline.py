"""Orchestrator: bronze -> silver (+DQ) -> consent stream -> gold, with a run log.

In Fabric: one Data Pipeline with these as notebook activities, retries, and failure
alerts; here a CLI so the whole thing runs on a laptop in a couple of minutes.

    python -m tlake.pipeline run
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from . import bronze, consent, gold, quality, settings, silver


def run() -> dict:
    t0 = time.time()
    runs = settings.read_state("run_log", [])
    batch_id = (runs[-1]["batch_id"] + 1) if runs else 1
    history = settings.read_state("ingestion_log", [])
    report = {"batch_id": batch_id, "started_at": datetime.now(timezone.utc).isoformat(),
              "as_of": settings.read_state("run_context", {}).get("as_of")}
    try:
        report["bronze"] = bronze.run(batch_id)
        report["volume_alerts"] = [a for e in report["bronze"]
                                   if (a := quality.volume_check(e["source"], e["rows"], history))]
        report["silver"] = silver.run(batch_id)
        report["consent_stream"] = consent.run_stream(batch_id)
        report["gold"] = gold.build()
        report["status"] = "succeeded"
    except quality.DataQualityError as e:
        report["status"] = "failed_data_quality"
        report["error"] = str(e)
        raise
    finally:
        report["seconds"] = round(time.time() - t0, 1)
        dq = [r for r in settings.read_state("dq_results", []) if r["batch_id"] == batch_id and r["failed"]]
        report["dq_failures"] = dq
        settings.write_state("run_log", runs + [report])
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["run"])
    ap.parse_args()
    print(json.dumps(run(), indent=2, default=str))


if __name__ == "__main__":
    main()
