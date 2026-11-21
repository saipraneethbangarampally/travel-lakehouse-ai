"""Paths and config loading. Everything is relative to TLAKE_HOME (default: repo root).

On Microsoft Fabric, TLAKE_HOME would point at the Lakehouse mount
(/lakehouse/default/Files) and TLAKE_TABLE_FORMAT=delta.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

import yaml

HOME = Path(os.environ.get("TLAKE_HOME", Path(__file__).resolve().parents[2]))
CONFIG = HOME / "config"
LAKE = Path(os.environ.get("TLAKE_LAKE", HOME / "lakehouse"))
SOURCES = Path(os.environ.get("TLAKE_SOURCES", HOME / "sources"))
EXPORTS = Path(os.environ.get("TLAKE_EXPORTS", HOME / "exports"))
STATE = LAKE / "_state"
CHECKPOINTS = LAKE / "_checkpoints"
KNOWLEDGE = HOME / "knowledge"

TABLE_FORMAT = os.environ.get("TLAKE_TABLE_FORMAT", "parquet")  # parquet | delta


@lru_cache
def load_yaml(name: str) -> dict:
    with open(CONFIG / name, encoding="utf-8") as f:
        return yaml.safe_load(f)


def sources_cfg() -> dict:
    return load_yaml("sources.yaml")


def quality_cfg() -> dict:
    return load_yaml("quality.yaml")


def consent_cfg() -> dict:
    return load_yaml("consent_policy.yaml")


def semantic_cfg() -> dict:
    return load_yaml("semantic_model.yaml")


# --- tiny JSON state store (watermarks, processed files, run log) ---------------------

def read_state(name: str, default):
    p = STATE / f"{name}.json"
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def write_state(name: str, value) -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    p = STATE / f"{name}.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    tmp.replace(p)  # atomic swap so a crash never leaves a half-written watermark
