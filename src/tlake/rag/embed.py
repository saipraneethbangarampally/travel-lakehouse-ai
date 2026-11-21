"""Embedders. The index records which one built it (and its fitted state), so a query is
always embedded the same way the documents were.

* LocalTfidfEmbedder: no network, no model download, deterministic. A sparse lexical
  model (unigram+bigram TF-IDF) fitted on the corpus and persisted with the index. It shows
  the retrieval mechanics honestly; swap in a semantic model for production quality.
* AzureOpenAIEmbedder: used automatically when AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY
  and AZURE_OPENAI_EMBEDDING_DEPLOYMENT are set (pip install openai).
"""
from __future__ import annotations

import math
import os
import re
from collections import Counter

import numpy as np
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

_TOKEN = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> list[str]:
    toks = [t for t in _TOKEN.findall(text.lower()) if t not in ENGLISH_STOP_WORDS and len(t) > 1]
    toks = [t[:-1] if len(t) > 3 and t.endswith("s") and not t.endswith("ss") else t for t in toks]
    return toks + [f"{a}_{b}" for a, b in zip(toks, toks[1:])]


class LocalTfidfEmbedder:
    name = "local-tfidf-v1"

    def __init__(self, vocab: dict[str, int] | None = None, idf: list[float] | None = None):
        self.vocab = vocab or {}
        self.idf = np.array(idf or [], dtype=np.float32)

    def fit(self, texts: list[str]) -> LocalTfidfEmbedder:
        df = Counter(t for text in texts for t in set(_tokens(text)))
        terms = sorted(df)
        self.vocab = {t: i for i, t in enumerate(terms)}
        n = len(texts)
        self.idf = np.array([math.log((1 + n) / (1 + df[t])) + 1 for t in terms], dtype=np.float32)
        return self

    def state(self) -> dict:
        return {"vocab": self.vocab, "idf": self.idf.tolist()}

    @property
    def dim(self) -> int:
        return len(self.vocab)

    def embed(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), max(self.dim, 1)), dtype=np.float32)
        for row, text in enumerate(texts):
            for term, tf in Counter(_tokens(text)).items():
                j = self.vocab.get(term)
                if j is not None:
                    out[row, j] = (1 + math.log(tf)) * self.idf[j]
        n = np.linalg.norm(out, axis=1, keepdims=True)
        return out / np.where(n == 0, 1, n)


class AzureOpenAIEmbedder:
    def __init__(self):
        from openai import AzureOpenAI
        self.deployment = os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"]
        self.name = f"azure-openai:{self.deployment}"
        self.client = AzureOpenAI(azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                                  api_key=os.environ["AZURE_OPENAI_API_KEY"],
                                  api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21"))

    def fit(self, texts):
        return self

    def state(self) -> dict:
        return {}

    def embed(self, texts: list[str]) -> np.ndarray:
        resp = self.client.embeddings.create(model=self.deployment, input=texts)
        v = np.array([d.embedding for d in resp.data], dtype=np.float32)
        return v / np.linalg.norm(v, axis=1, keepdims=True)


def default_embedder():
    if all(os.environ.get(k) for k in ("AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY",
                                       "AZURE_OPENAI_EMBEDDING_DEPLOYMENT")):
        return AzureOpenAIEmbedder()
    return LocalTfidfEmbedder()
