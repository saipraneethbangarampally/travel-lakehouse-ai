"""Governance driven by the semantic model: column classification -> what each role sees.

The same YAML produces three things:
  1. runtime enforcement for the serving layer / MCP tools (this module)
  2. T-SQL for a Fabric Warehouse: dynamic data masking + row-level security
  3. a sensitivity-label map to apply in Microsoft Purview
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from . import settings

LEVELS = ["public", "internal", "confidential", "pii"]
PURVIEW_LABEL = {"public": "Public", "internal": "General", "confidential": "Confidential",
                 "pii": "Highly Confidential - Personal Data"}


class AccessDenied(PermissionError):
    pass


@dataclass(frozen=True)
class Principal:
    id: str
    role: str
    partners: tuple[str, ...]

    def can_see_partner(self, code: str) -> bool:
        return "*" in self.partners or code in self.partners

    @property
    def policy(self) -> dict:
        return settings.semantic_cfg()["roles"][self.role]


def principal(pid: str) -> Principal:
    p = settings.semantic_cfg()["principals"].get(pid)
    if p is None:
        raise AccessDenied(f"unknown principal {pid!r}")
    return Principal(pid, p["role"], tuple(p["partners"]))


def dataset_spec(name: str) -> dict:
    ds = settings.semantic_cfg()["datasets"].get(name)
    if ds is None:
        raise KeyError(name)
    return ds


def visible_columns(name: str, who: Principal) -> dict[str, str]:
    """column -> 'allow' | 'mask' for this principal (denied columns are absent)."""
    pol = who.policy
    out = {}
    for col, meta in dataset_spec(name)["columns"].items():
        cls = meta["classification"]
        rule = pol.get(cls, "allow")
        if rule != "deny":
            out[col] = rule
    return out


def can_read_dataset(name: str, who: Principal) -> bool:
    ds = dataset_spec(name)
    return who.role != "ai_agent" or ds.get("ai_exposed", False)


def _mask_value(col: str, v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return v
    s = str(v)
    if col == "email" and "@" in s:
        user, dom = s.split("@", 1)
        return f"{user[:1]}***@{dom}"
    if col == "phone":
        return "***-" + s[-2:]
    return s[:1] + "***"


def secure_frame(df: pd.DataFrame, name: str, who: Principal) -> pd.DataFrame:
    """Row scope first (partner), then drop denied columns, then mask."""
    if not can_read_dataset(name, who):
        raise AccessDenied(f"{who.id} may not read {name}")
    if "partner_code" in df.columns and "*" not in who.partners:
        df = df[df["partner_code"].isin(who.partners)]
    cols = visible_columns(name, who)
    df = df[[c for c in df.columns if c in cols]].copy()
    for c, rule in cols.items():
        if rule == "mask" and c in df.columns:
            df[c] = df[c].map(lambda v, c=c: _mask_value(c, v))
    return df


# --------------------------------------------------------------- Fabric artefacts -----

def render_fabric_sql(schema: str = "gold") -> str:
    """T-SQL for a Fabric Warehouse / SQL endpoint generated from the semantic model."""
    lines = ["-- Generated from config/semantic_model.yaml. Do not edit by hand.",
             "-- 1) Dynamic data masking on PII columns", ""]
    for name, ds in settings.semantic_cfg()["datasets"].items():
        for col, meta in ds["columns"].items():
            if meta["classification"] == "pii":
                fn = "email()" if col == "email" else 'partial(1,"***",0)'
                lines.append(f"ALTER TABLE {schema}.{name} ALTER COLUMN {col} ADD MASKED WITH (FUNCTION = '{fn}');")
    lines += ["", "-- Only marketing ops may unmask (activation of approved audiences)",
              "GRANT UNMASK TO [role_marketing_ops];", "",
              "-- 2) Row-level security: a principal only sees its partners' rows",
              "CREATE SCHEMA security;", "GO",
              "CREATE TABLE security.principal_partner (principal_name sysname, partner_code varchar(10));",
              "GO",
              "CREATE FUNCTION security.fn_partner_predicate(@partner_code varchar(10))",
              "RETURNS TABLE WITH SCHEMABINDING AS",
              "RETURN SELECT 1 AS allowed FROM security.principal_partner p",
              "       WHERE p.principal_name = USER_NAME() AND (p.partner_code = @partner_code OR p.partner_code = '*');",
              "GO", "CREATE SECURITY POLICY security.partner_isolation"]
    preds = [f"    ADD FILTER PREDICATE security.fn_partner_predicate(partner_code) ON {schema}.{n}"
             for n, ds in settings.semantic_cfg()["datasets"].items() if "partner_code" in ds["columns"]]
    lines.append(",\n".join(preds))
    lines += ["    WITH (STATE = ON);", ""]
    lines.append("-- 3) Purview sensitivity labels (apply via Purview / Fabric item labels)")
    for name, ds in settings.semantic_cfg()["datasets"].items():
        top = max((LEVELS.index(m["classification"]) for m in ds["columns"].values()))
        lines.append(f"--   {schema}.{name}: {PURVIEW_LABEL[LEVELS[top]]}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import sys
    sys.stdout.write(render_fabric_sql())
