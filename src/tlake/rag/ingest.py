"""Document ingestion for RAG: parse -> redact PII -> chunk -> scan -> embed -> index.

Security happens at ingestion, not only at question time:
  * PII (emails, phone numbers) is redacted BEFORE embedding, so it can never be
    retrieved, cited or sent to a model
  * every chunk carries the document's partner_scope ACL, enforced before similarity search
  * chunks that look like instructions to a model (prompt injection) are quarantined:
    indexed for audit, never used as context

In Fabric/Azure the index would be Azure AI Search (or an Eventhouse vector column); the
interface here (chunk records + vectors + manifest) maps 1:1.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml

from .. import settings
from .embed import default_embedder

INDEX_DIR = settings.LAKE / "vector_index"
CHUNK_CHARS = 700
OVERLAP_CHARS = 120

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE = re.compile(r"\+?\d[\d\-\s().]{7,}\d")
# High-severity patterns quarantine a chunk on their own. Low-severity ones (a doc that
# legitimately says "use the data_freshness tool") only quarantine in combination.
INJECTION_HIGH = [
    r"ignore (all |any )?(previous|prior|above|earlier) (instructions|rules|prompts)",
    r"disregard (the |all |any )?(previous |prior )?(instructions|rules|guidelines)",
    r"\byou are now\b",
    r"\badmin mode\b",
    r"do not (tell|inform|mention to) the user",
    r"</?\s*(system|assistant|tool)\s*>",
]
INJECTION_LOW = [
    r"\b(system|developer) (prompt|instruction|message)",
    r"\b(call|use|invoke) the \w+ tool\b",
    r"\bsend (the |all |every )?.{0,40}(list|data|emails?|records)\b.{0,40}\bto\b",
]
HIGH_RE = re.compile("|".join(INJECTION_HIGH), re.IGNORECASE)
LOW_RE = re.compile("|".join(INJECTION_LOW), re.IGNORECASE)


def injection_findings(text: str) -> list[str]:
    return sorted({m.group(0).lower() for r in (HIGH_RE, LOW_RE) for m in r.finditer(text)})


def is_injection(text: str) -> bool:
    return bool(HIGH_RE.search(text)) or len({m.group(0).lower() for m in LOW_RE.finditer(text)}) >= 2


def parse(path: Path) -> tuple[dict, str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        raise ValueError(f"{path.name}: missing front matter")
    _, fm, body = text.split("---", 2)
    meta = yaml.safe_load(fm)
    for k in ("doc_id", "title", "partner_scope"):
        if k not in meta:
            raise ValueError(f"{path.name}: front matter needs {k}")
    return meta, body.strip()


def redact(text: str) -> tuple[str, int]:
    text, n1 = EMAIL.subn("[REDACTED_EMAIL]", text)
    text, n2 = PHONE.subn("[REDACTED_PHONE]", text)
    return text, n1 + n2


def chunk(body: str) -> list[tuple[str, str]]:
    """Split on markdown headings (keeps sections coherent), then window long sections
    on sentence boundaries with overlap. Returns (section_title, text)."""
    sections, title, buf = [], "Introduction", []
    for line in body.splitlines():
        h = re.match(r"^#{1,6}\s+(.*)", line)
        if h:
            if "".join(buf).strip():
                sections.append((title, "\n".join(buf).strip()))
            title, buf = h.group(1).strip(), []
        else:
            buf.append(line)
    if "".join(buf).strip():
        sections.append((title, "\n".join(buf).strip()))

    out = []
    for title, text in sections:
        if len(text) <= CHUNK_CHARS:
            out.append((title, text))
            continue
        sentences = re.split(r"(?<=[.!?])\s+", text)
        cur = ""
        for s in sentences:
            if cur and len(cur) + len(s) > CHUNK_CHARS:
                out.append((title, cur.strip()))
                cur = cur[-OVERLAP_CHARS:]
            cur += " " + s
        if cur.strip():
            out.append((title, cur.strip()))
    return out


def build_index(folder: Path | None = None, embedder=None) -> dict:
    folder = folder or settings.KNOWLEDGE
    embedder = embedder or default_embedder()
    records, texts = [], []
    stats = {"documents": 0, "chunks": 0, "redactions": 0, "quarantined_chunks": 0}
    for path in sorted(folder.glob("*.md")):
        meta, body = parse(path)
        stats["documents"] += 1
        for i, (section, text) in enumerate(chunk(body)):
            clean, n_red = redact(text)
            findings = injection_findings(clean)
            quarantined = is_injection(clean)
            stats["redactions"] += n_red
            stats["quarantined_chunks"] += quarantined
            records.append({
                "chunk_id": f"{meta['doc_id']}#{i}",
                "doc_id": meta["doc_id"], "title": meta["title"], "section": section,
                "partner_scope": meta["partner_scope"], "classification": meta.get("classification", "internal"),
                "updated": str(meta.get("updated", "")), "text": clean,
                "quarantined": quarantined, "injection_findings": findings,
                "sha256": hashlib.sha256(clean.encode()).hexdigest()[:16],
            })
            # embed title + section + text: headings carry a lot of meaning in short docs
            texts.append(f"{meta['title']}. {section}. {clean}")
    stats["chunks"] = len(records)
    # fit only on chunks that will be served, so quarantined text can't shape the model
    embedder.fit([t for t, r in zip(texts, records) if not r["quarantined"]])
    vectors = embedder.embed(texts) if texts else np.zeros((0, 1), dtype=np.float32)

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    with open(INDEX_DIR / "chunks.jsonl", "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    np.save(INDEX_DIR / "vectors.npy", vectors)
    (INDEX_DIR / "embedder_state.json").write_text(json.dumps(embedder.state()), encoding="utf-8")
    manifest = {"embedder": embedder.name, "dim": int(vectors.shape[1]), **stats,
                "built_at": datetime.now(timezone.utc).isoformat()}
    (INDEX_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    print(json.dumps(build_index(), indent=2))
