"""Governed serving layer over gold: what BI tools and AI agents are allowed to ask.

There is deliberately NO "run this SQL" function. Agents pick a metric that is defined in
the semantic model, plus optional group-bys/filters from an allow-list. That removes SQL
injection, keeps numbers consistent with Power BI, and lets us enforce partner isolation
and small-group suppression in one place.

Reads gold with pandas/pyarrow (the stand-in for the Fabric SQL analytics endpoint).
"""
from __future__ import annotations

from functools import lru_cache

import pandas as pd

from . import audience, governance, settings

MAX_GROUP_BY = 3
MAX_FILTER_VALUES = 50
MAX_ROWS = 200


class InvalidRequest(ValueError):
    pass


def load(name: str) -> pd.DataFrame:
    path = settings.LAKE / "gold" / name
    if not path.exists():
        raise FileNotFoundError(f"gold table {name} not built yet; run the pipeline")
    # cache per table version: any rewrite changes a file mtime, which invalidates the entry
    stamp = max((f.stat().st_mtime_ns for f in path.rglob("*") if f.is_file()), default=0)
    return _load_cached(str(path), settings.TABLE_FORMAT, stamp).copy()


@lru_cache(maxsize=16)
def _load_cached(path: str, fmt: str, stamp: int) -> pd.DataFrame:
    if fmt == "delta":
        from deltalake import DeltaTable
        df = DeltaTable(path).to_pandas()
    else:
        df = pd.read_parquet(path)
    for c in df.columns:  # hive partition columns come back as categoricals
        if isinstance(df[c].dtype, pd.CategoricalDtype):
            df[c] = df[c].astype(str)
    return df


def _with_derived(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    if dataset == "fact_booking":
        d = pd.to_datetime(df["booking_date"])
        df = df.assign(booking_month=d.dt.strftime("%Y-%m"), booking_year=d.dt.year.astype(str))
    if dataset == "fact_email_engagement":
        df = df.assign(activity_month=pd.to_datetime(df["activity_date"]).dt.strftime("%Y-%m"))
    if dataset == "fact_web_event":
        df = df.assign(event_month=pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m"))
    return df


def _scope(df: pd.DataFrame, who: governance.Principal) -> pd.DataFrame:
    if "*" in who.partners:
        return df
    return df[df["partner_code"].isin(who.partners)]


def _validate_filters(filters: dict | None, allowed: list[str]) -> dict[str, list[str]]:
    out = {}
    for k, v in (filters or {}).items():
        if k not in allowed:
            raise InvalidRequest(f"cannot filter on {k!r}; allowed: {allowed}")
        vals = v if isinstance(v, list) else [v]
        if not vals or len(vals) > MAX_FILTER_VALUES or not all(isinstance(x, (str, int)) for x in vals):
            raise InvalidRequest(f"filter {k!r} needs 1-{MAX_FILTER_VALUES} string values")
        out[k] = [str(x) for x in vals]
    return out


# ------------------------------------------------------------------------- tools ------

def list_datasets(who: governance.Principal) -> list[dict]:
    sem = settings.semantic_cfg()
    return [{"name": n, "description": d["description"], "grain": d["grain"]}
            for n, d in sem["datasets"].items() if governance.can_read_dataset(n, who)]


def describe_dataset(name: str, who: governance.Principal) -> dict:
    try:
        ds = governance.dataset_spec(name)
    except KeyError:
        raise InvalidRequest(f"unknown dataset {name!r}") from None
    if not governance.can_read_dataset(name, who):
        raise governance.AccessDenied(f"{name} is not exposed to {who.role}")
    visible = governance.visible_columns(name, who)
    sem = settings.semantic_cfg()
    return {
        "name": name, "description": ds["description"], "grain": ds["grain"],
        "columns": {c: {"type": m["type"], "description": m["description"],
                        **({"masked": True} if visible.get(c) == "mask" else {})}
                    for c, m in ds["columns"].items() if c in visible},
        "withheld_columns": len(ds["columns"]) - len(visible),
        "groupable_dimensions": sem["dimensions"].get(name, []),
        "metrics": sorted(k for k, m in sem["metrics"].items() if m["dataset"] == name),
    }


def list_metrics() -> list[dict]:
    return [{"metric": k, "description": m["description"], "dataset": m["dataset"]}
            for k, m in settings.semantic_cfg()["metrics"].items()]


def query_metric(metric: str, who: governance.Principal, group_by: list[str] | None = None,
                 filters: dict | None = None) -> dict:
    sem = settings.semantic_cfg()
    spec = sem["metrics"].get(metric)
    if spec is None:
        raise InvalidRequest(f"unknown metric {metric!r}; call list_metrics")
    ds = spec["dataset"]
    if not governance.can_read_dataset(ds, who) and who.role == "ai_agent":
        raise governance.AccessDenied(f"{ds} not exposed to agents")
    allowed = sem["dimensions"][ds]
    group_by = list(group_by or [])
    if len(group_by) > MAX_GROUP_BY or any(g not in allowed for g in group_by):
        raise InvalidRequest(f"group_by must be up to {MAX_GROUP_BY} of {allowed}")
    flt = _validate_filters(filters, allowed)

    df = _with_derived(_scope(load(ds), who), ds)
    for k, vals in flt.items():
        df = df[df[k].astype(str).isin(vals)]

    def agg(frame: pd.DataFrame) -> pd.Series:
        members = frame["member_key"].nunique() if "member_key" in frame else None
        a = spec["agg"]
        if a == "ratio":
            num = len(frame.query(spec["numerator_where"]))
            den_frame = frame.query(spec["denominator_where"]) if "denominator_where" in spec else frame
            val = round(num / len(den_frame), 4) if len(den_frame) else None
        else:
            sub = frame.query(spec["where"]) if "where" in spec else frame
            if a == "count":
                val = len(sub)
            elif a == "sum":
                val = round(float(sub[spec["column"]].astype(float).sum()), 2)
            elif a == "mean":
                val = round(float(sub[spec["column"]].astype(float).mean()), 2) if len(sub) else None
            else:
                raise InvalidRequest(f"unsupported agg {a}")
        return pd.Series({"value": val, "member_count": members})

    if group_by:
        res = df.groupby(group_by, dropna=False).apply(agg, include_groups=False).reset_index()
    else:
        res = agg(df).to_frame().T
    k = who.policy.get("min_group_size")
    suppressed = 0
    rows = []
    for r in res.to_dict("records"):
        mc = r.pop("member_count")
        if k and mc is not None and mc < k:
            r["value"] = None
            r["suppressed"] = f"fewer than {k} members"
            suppressed += 1
        rows.append({kk: (None if pd.isna(vv) else vv) if not isinstance(vv, (list, dict)) else vv
                     for kk, vv in r.items()})
    rows = rows[:MAX_ROWS]
    return {"metric": metric, "definition": spec["description"], "group_by": group_by,
            "filters": flt, "partner_scope": list(who.partners), "rows": rows,
            "suppressed_groups": suppressed, "as_of": _as_of()}


def estimate_audience(partner_code: str, criteria: dict, who: governance.Principal) -> dict:
    """Counts only (no member list): how many match, how many are suppressed and why."""
    if not who.can_see_partner(partner_code):
        raise governance.AccessDenied(f"{who.id} is not scoped to partner {partner_code}")
    try:
        triples = audience.compile_criteria(criteria)
    except ValueError as e:
        raise InvalidRequest(str(e)) from None
    m = load("member_360")
    m = m[m["partner_code"] == partner_code]
    hit = m[audience.pandas_mask(m, triples)]
    contact = load("member_contactability")[["member_key", "suppression_reason"]]
    hit = hit.merge(contact, on="member_key", how="left")
    reasons = hit["suppression_reason"].fillna("marketable").value_counts().to_dict()
    k = who.policy.get("min_group_size") or 0

    def safe(n: int):
        return n if n >= k else f"<{k}"

    return {"partner_code": partner_code, "criteria": criteria,
            "matched_criteria": safe(len(hit)),
            "marketable": safe(int(reasons.pop("marketable", 0))),
            "suppressed": {r: safe(int(n)) for r, n in reasons.items()},
            "as_of": _as_of()}


def data_freshness() -> dict:
    runs = settings.read_state("run_log", [])
    ok = [r for r in runs if r.get("status") == "succeeded"]
    last = ok[-1] if ok else None
    return {"as_of": _as_of(), "last_successful_batch": last and last["batch_id"],
            "last_run_finished": last and last["started_at"],
            "last_run_status": runs[-1].get("status") if runs else None,
            "open_dq_issues": [f"{d['entity']}.{d['rule']}: {d['failed']} rows ({d['severity']})"
                               for d in (last or {}).get("dq_failures", [])]}


def _as_of():
    return settings.read_state("run_context", {}).get("as_of")
