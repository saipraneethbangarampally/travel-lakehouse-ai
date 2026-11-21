# Mapping to Microsoft Fabric

| This repo | Fabric item | Notes |
|---|---|---|
| `config/sources.yaml` + `bronze.py` | **Data Pipeline** (Lookup → ForEach → Copy activity) through the **on-premises data gateway** | Same metadata drives the pipeline; watermarks in a control table. Mirroring for SQL Server is an alternative where supported |
| `lakehouse/bronze`, `silver`, `gold` | **Lakehouse** tables (Delta in OneLake), one lakehouse per layer or schemas per layer | `TLAKE_TABLE_FORMAT=delta` |
| `silver.py`, `gold.py`, `quality.py` | **Notebooks** (PySpark / Spark SQL) as pipeline activities | `lake.upsert` → `MERGE INTO`; V-Order on gold |
| `consent.py` | **Eventstream** (source: Event Hubs / custom app) → Lakehouse, or Spark Structured Streaming notebook | Checkpoint in OneLake; continuous trigger |
| `pipeline.py` | **Data Pipeline** orchestration with retries, failure alerts (Teams/email), scheduled 02:00 UTC | Run log table for SLA monitoring |
| `config/semantic_model.yaml` | **Power BI semantic model** (Direct Lake on gold) descriptions + measures; **Purview** classifications | Measures match `metrics:` 1:1 |
| `governance.render_fabric_sql()` | **Warehouse / SQL analytics endpoint**: dynamic data masking, row-level security; **OneLake data access roles** | Generated, checked in CI |
| Sensitivity map | **Microsoft Purview** sensitivity labels on items, inherited downstream | `dim_member` → Highly Confidential |
| `serving.py` + `mcp_server.py` | MCP server on Azure Container Apps / Functions reading the SQL endpoint; **Fabric data agent** for in-Fabric Q&A | Managed identity per partner deployment |
| `rag/` | **Azure AI Search** (vector + filters on `partner_scope`) + **Azure OpenAI** | Or vector columns in Eventhouse |
| `.github/workflows/ci.yml` | **Git integration** (GitHub / Azure DevOps) + **Deployment pipelines** Dev → Test → Prod | See below |

## CI/CD and branching

```
feature/* ──PR──▶ main ──(Fabric Git sync)──▶ Dev workspace
                                   │ deployment pipeline + deployment rules (connections, lakehouse ids)
                                   ▼
                              Test workspace ──(approval)──▶ Prod workspace
```

* PRs run lint, unit and end-to-end tests on synthetic data (this repo's CI).
* Fabric items (notebooks, pipelines, semantic model) are source-controlled through Git integration.
* Deployment rules swap environment-specific connections, so no hard-coded workspace ids.
* Generated artefacts (security SQL) are checked against their source of truth in CI.

## Example Fabric notebook cell: silver bookings MERGE

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

batch = (spark.read.table("bronze.sam__reservation").filter(F.col("_batch_id") > last_batch)
         .selectExpr(*[f"{expr} AS {col}" for col, expr in mapping.items()]))
w = Window.partitionBy("booking_key").orderBy(F.col("modified_at").desc())
batch = batch.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

(DeltaTable.forName(spark, "silver.bookings").alias("t")
   .merge(batch.alias("s"), "t.booking_key = s.booking_key")
   .whenMatchedUpdateAll(condition="s.modified_at >= t.modified_at")
   .whenNotMatchedInsertAll()
   .execute())
```
