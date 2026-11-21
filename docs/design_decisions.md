# Design decisions and trade-offs

## 1. Ingestion: incremental, at-least-once, idempotent downstream

* **High-watermark per source table**, compared with `>=`. A strict `>` can miss rows
  committed in the same second as the previous extract. With `>=` some boundary rows are
  delivered twice, and silver absorbs that because every write is keyed and idempotent
  (`test_rerun_without_new_data_is_idempotent`).
* The **watermark is advanced only after the bronze write succeeds**, so a failed write
  just gets retried.
* **Bronze is schema-on-read**: every column lands as a string, plus `_batch_id`,
  `_ingested_at` and `_source_system`. Upstream type drift (a source changes `REAL` to `TEXT`)
  can't break ingestion. Typing happens in silver, under a contract.
* **Metadata-driven**: partner differences (column names, date formats, cents vs dollars,
  ISO-3 vs ISO-2 country codes, tier codes) are Spark SQL expressions in
  `config/sources.yaml`. In Fabric the same file drives a Data Pipeline `ForEach` over Copy
  activities.
* Where a source can't provide a reliable modified timestamp, use CDC (SQL Server CDC /
  Fabric mirroring where supported) rather than a watermark.

## 2. Silver: one canonical contract

* **Keys**: `member_key = sha256(partner_code | natural_id)`. It's stable and deterministic, and
  the partner prefix enforces isolation: two partners' member `1001` can never collide or
  be merged. We **don't** do cross-partner identity resolution on purpose. White-label
  partners own their customers.
* **SCD Type 2** on `tier, country, state, email_hash`: these change what we may do (consent
  jurisdiction) and how we report (tier at time of booking). Name and phone fixes are
  **Type 1** and overwrite the current version. The first version starts at `1900-01-01`, so
  point-in-time joins never fall off the start of history.
* **Point-in-time join** in `fact_booking`: a booking made while the member was SILVER stays
  SILVER in reports after they're upgraded to GOLD.
* Emails are normalised (trim + lowercase) and **hashed in silver**. Raw email exists only in
  `dim_member`, the one table that gets masking and the strictest label.

### MERGE equivalents at scale (Delta / Fabric)

```sql
-- bookings: latest version wins
MERGE INTO silver.bookings t
USING (SELECT * FROM batch QUALIFY row_number() OVER (PARTITION BY booking_key ORDER BY modified_at DESC) = 1) s
ON t.booking_key = s.booking_key
WHEN MATCHED AND s.modified_at >= t.modified_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- members SCD2: close changed current rows, then insert new versions
MERGE INTO silver.members t
USING changes s ON t.member_key = s.member_key AND t.is_current
WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN
  UPDATE SET valid_to = s.modified_at, is_current = false;
INSERT INTO silver.members SELECT ... FROM changes s
  LEFT ANTI JOIN silver.members t ON t.member_key = s.member_key AND t.is_current AND t.attr_hash = s.attr_hash;
```

Locally the same semantics come from `lake.upsert` / `silver.scd2_merge` (pure functions,
unit tested) followed by a staged overwrite. That's fine at demo scale. At production scale I'd
use MERGE, partition big facts by date, and use liquid clustering / V-Order on hot filter
columns (`partner_code`, `booking_date`).

## 3. Data quality as a gate, not a report

| Severity | Effect | Example |
|---|---|---|
| `error` | stop the run, publish nothing | missing primary key, unparseable modified timestamp |
| `quarantine` | hold the row with the rule names, continue | negative amount, unsupported currency, unknown country |
| `warn` | log only | malformed email (the member is still valid; `email_is_valid=false` makes them unmarketable) |

* **Late-arriving dimensions**: on-prem replication lag means a booking can arrive before
  its member. Those rows are quarantined as `member_exists` and **retried every run**. In
  the demo, 28 resolve on day 2.
* **Fixes flow through**: when the source corrects a negative amount, the new version passes
  and the key is removed from quarantine (`test_source_fix_leaves_quarantine`).
* Rule results are logged per batch, and `data_freshness` exposes open issues to users and agents.

## 4. Consent

* **Stream, not batch**: an opt-out must apply before the next send. Structured Streaming
  with checkpoints (Eventstream in Fabric). `availableNow` locally, continuous in production.
* **Event time, not arrival time**: a 400-day-old opt-in backfilled from a legacy CRM must not
  override yesterday's opt-out.
* **Ties go restrictive**: if opt-in and opt-out share a timestamp, opt-out wins.
* **Jurisdiction model**: EU/UK/Canada need an explicit opt-in. US/Mexico are opt-out based.
  An unknown country defaults to the stricter model.
* **Checked twice**: when the audience is built, and again at activation export. This is tested
  by streaming an opt-out *after* the build (`test_export_rechecks_consent_at_activation`).

## 5. AI-ready by design (instead of a second layer)

What makes the gold layer usable by AI, beyond being clean:

1. **Business descriptions on every table and column.** The same text becomes Power BI
   descriptions and the context `describe_dataset` gives an agent. Models do much better
   when they're told what `cruise_affinity` means and how it's computed.
2. **Governed metrics.** Agents can't invent "revenue". They call `gross_booking_value_usd`,
   whose definition (excludes cancellations, USD-normalised) is the one finance agreed on.
3. **Wide, documented feature tables** (`member_360`) at a clear grain, so an agent doesn't
   have to work out joins.
4. **Classification on every column**, so access rules are data, not code.
5. **Freshness and quality are queryable**, so an agent can say "as of 2 Nov, with 1 record held
   for a data issue" instead of presenting numbers as absolute truth.

## 6. MCP server security model

| Threat | Control |
|---|---|
| Model writes harmful or expensive SQL | No SQL tool. Typed metric API with allow-listed dimensions, max 3 group-bys, row caps |
| Model asks for another partner's data | Principal set by deployment (`TLAKE_PRINCIPAL`), never a tool argument. Row scope enforced server-side |
| Re-identification via tiny groups | k-anonymity: groups with < 10 members are suppressed |
| PII leakage | PII and confidential columns never reach `ai_agent`. `dim_member` isn't exposed. Audiences return counts only |
| Destructive actions | All tools are read-only and annotated as such. Export is a separate, human, role-gated path |
| Untrusted text steering the client model | Outputs are wrapped as data (`untrusted_text`, `note`), plus server instructions |
| No accountability | Every call is audit-logged with principal and arguments |

## 7. RAG and prompt injection: defence in depth

No single control stops prompt injection, so there are several layers:

1. **Minimise what can reach the model**: PII is redacted *before* embedding, so it can't be
   retrieved, cited or leaked.
2. **Permission-aware retrieval**: the partner ACL filters chunks *before* similarity. A SAM
   assistant asking about BlueHarbor gets "I don't know", not a ranked snippet.
3. **Quarantine suspicious content at ingestion**: high-severity patterns ("ignore previous
   instructions", "admin mode", role tags) quarantine on their own. Low-severity ones
   (mentions of tools, "send ... to") only quarantine in combination, so a policy saying "use the
   data_freshness tool" isn't a false positive. Quarantined chunks are kept for audit and
   never served.
4. **Prompt structure**: retrieved text sits inside `<context>` delimiters, and the system prompt
   says it's data and must never be followed.
5. **Least privilege for tools**: even a successful injection can only call read-only,
   partner-scoped, PII-free tools. There's nothing to exfiltrate and no way to act.
6. **Output checks**: answers must cite retrieved chunk ids. Anything else is withheld.
7. **Abstain**: below a similarity threshold the answer is "I don't know".

The regex detector is a tripwire, not a guarantee. In production I'd add a classifier
(e.g. Azure AI Content Safety Prompt Shields) and red-team test sets in CI.

## 8. What I would add next

* Delta `MERGE` + liquid clustering, and Direct Lake semantic model on gold
* Eventstream from Event Hubs for consent, with a dead-letter table for malformed events
* Great Expectations / Fabric data quality rules synced from `quality.yaml`
* Frequency-cap enforcement (2 promos per 7 days) as a gold table joined at export
* Evaluation set for RAG (questions → expected citations) run in CI
* Fabric data agent on the same semantic model, with this MCP server for external assistants
