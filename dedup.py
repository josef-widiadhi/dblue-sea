"""
DB Blueprint v2 — Similarity & Deduplication Engine
=====================================================
Three-pass analysis:
  1. Name similarity   — fuzzy match on table/column names
  2. Value overlap     — sample data intersection between FK candidates
  3. Structural fingerprint — tables with similar column structure
"""

from __future__ import annotations
import re
from collections import defaultdict
from typing import Any


# ─── Helpers ──────────────────────────────────────────────────

def _normalise(name: str) -> str:
    """Lowercase, strip underscores/prefixes, singularise naively."""
    n = name.lower().strip()
    n = re.sub(r"[_\-\s]+", " ", n).strip()
    # Strip common prefixes: tbl_, t_, m_, v_
    n = re.sub(r"^(tbl|t|m|v|tb|ref|mst|trx|tmp|temp)\s+", "", n)
    # Naive singularise
    if n.endswith("ies"):
        n = n[:-3] + "y"
    elif n.endswith("ses") or n.endswith("xes"):
        n = n[:-2]
    elif n.endswith("s") and len(n) > 3:
        n = n[:-1]
    return n


def _token_set(name: str) -> set[str]:
    tokens = re.split(r"[_\-\s]+", name.lower())
    return {t for t in tokens if len(t) > 1}


def _similarity(a: str, b: str) -> float:
    """Combine token-set Jaccard with sequence ratio."""
    try:
        from rapidfuzz import fuzz
        seq = fuzz.ratio(a.lower(), b.lower()) / 100.0
    except ImportError:
        from difflib import SequenceMatcher
        seq = SequenceMatcher(None, a.lower(), b.lower()).ratio()

    ta, tb = _token_set(a), _token_set(b)
    if not ta and not tb:
        jaccard = 1.0
    elif not ta or not tb:
        jaccard = 0.0
    else:
        jaccard = len(ta & tb) / len(ta | tb)

    # Also compare normalised forms
    na, nb = _normalise(a), _normalise(b)
    try:
        from rapidfuzz import fuzz
        norm_sim = fuzz.ratio(na, nb) / 100.0
    except ImportError:
        from difflib import SequenceMatcher
        norm_sim = SequenceMatcher(None, na, nb).ratio()

    return max(seq, jaccard, norm_sim)


# ─── Pass 1: Name similarity ──────────────────────────────────

def find_similar_tables(schema: dict, threshold: float = 0.72) -> list[dict]:
    """Find table pairs that look like duplicates or aliases."""
    tables = list(schema.get("tables", {}).keys())
    results = []
    seen = set()
    for i, a in enumerate(tables):
        for b in tables[i+1:]:
            key = tuple(sorted([a, b]))
            if key in seen:
                continue
            seen.add(key)
            score = _similarity(a, b)
            if score >= threshold:
                results.append({
                    "type":       "similar_tables",
                    "table_a":    a,
                    "table_b":    b,
                    "score":      round(score, 3),
                    "confidence": "high" if score >= 0.9 else "medium",
                    "reason":     f"Name similarity {round(score*100)}% — possible duplicate or alias",
                })
    return sorted(results, key=lambda x: -x["score"])


def find_similar_columns(schema: dict, threshold: float = 0.80) -> list[dict]:
    """
    Find columns across tables that look like they should be FK relations.
    Specifically: columns ending in _id, _cd, _no, _key that closely match
    a PK column in another table.
    """
    tables = schema.get("tables", {})
    # Build PK index: {table_name: [pk_col_name, ...]}
    pk_index: dict[str, list[str]] = {}
    for tname, tdata in tables.items():
        pks = [c["column_name"] for c in tdata.get("columns", []) if c.get("is_pk")]
        if pks:
            pk_index[tname] = pks

    results = []
    fk_patterns = re.compile(r"(.+?)(_id|_cd|_no|_key|_code|_ref|Id|Cd|No|Key)$", re.IGNORECASE)

    for src_table, tdata in tables.items():
        for col in tdata.get("columns", []):
            cname = col["column_name"]
            m = fk_patterns.match(cname)
            if not m:
                continue
            stem = m.group(1)   # e.g. "user" from "user_id"

            for ref_table, pk_cols in pk_index.items():
                if ref_table == src_table:
                    continue
                for pk_col in pk_cols:
                    # Score stem against ref_table name
                    score = _similarity(stem, ref_table)
                    if score >= threshold:
                        results.append({
                            "type":       "inferred_fk",
                            "from_table": src_table,
                            "from_col":   cname,
                            "to_table":   ref_table,
                            "to_col":     pk_col,
                            "score":      round(score, 3),
                            "confidence": "high" if score >= 0.92 else "medium",
                            "reason":     f"Column stem '{stem}' matches table '{ref_table}' ({round(score*100)}% similarity)",
                        })

    # Deduplicate: keep highest score per (from_table, from_col) pair
    best: dict[tuple, dict] = {}
    for r in results:
        key = (r["from_table"], r["from_col"])
        if key not in best or r["score"] > best[key]["score"]:
            best[key] = r
    return sorted(best.values(), key=lambda x: -x["score"])


# ─── Pass 2: Value overlap ────────────────────────────────────

def find_value_overlaps(schema: dict, min_overlap: float = 0.4) -> list[dict]:
    """
    For candidate FK columns, compare sampled values against PK columns.
    High intersection = strong FK evidence.
    """
    tables = schema.get("tables", {})
    results = []

    # Build value index for PK columns
    pk_values: dict[tuple, set] = {}   # (table, col) → set of values
    for tname, tdata in tables.items():
        for col in tdata.get("columns", []):
            if col.get("is_pk"):
                samples = tdata.get("samples", [])
                vals = {str(row.get(col["column_name"], "")) for row in samples if row.get(col["column_name"])}
                if vals:
                    pk_values[(tname, col["column_name"])] = vals

    fk_patterns = re.compile(r"(.+?)(_id|_cd|_no|_key|_code|_ref|Id|Cd|No|Key)$", re.IGNORECASE)

    for src_table, tdata in tables.items():
        for col in tdata.get("columns", []):
            cname = col["column_name"]
            if col.get("is_pk"):
                continue
            # Get sampled values for this column
            src_vals = {str(row.get(cname, "")) for row in tdata.get("samples", []) if row.get(cname)}
            if not src_vals:
                continue

            for (ref_table, ref_col), ref_vals in pk_values.items():
                if ref_table == src_table:
                    continue
                if not ref_vals:
                    continue
                overlap = len(src_vals & ref_vals) / max(len(src_vals), 1)
                if overlap >= min_overlap:
                    results.append({
                        "type":       "value_overlap",
                        "from_table": src_table,
                        "from_col":   cname,
                        "to_table":   ref_table,
                        "to_col":     ref_col,
                        "overlap_pct": round(overlap * 100, 1),
                        "score":      round(overlap, 3),
                        "confidence": "high" if overlap >= 0.7 else "medium",
                        "reason":     f"{round(overlap*100)}% value overlap between sample sets",
                    })

    return sorted(results, key=lambda x: -x["score"])


# ─── Pass 3: Structural fingerprint ──────────────────────────

def find_structural_duplicates(schema: dict, threshold: float = 0.85) -> list[dict]:
    """Tables with very similar column type distributions."""
    tables = schema.get("tables", {})

    def fingerprint(tdata: dict) -> tuple:
        types = sorted(c["data_type"].lower() for c in tdata.get("columns", []))
        return tuple(types)

    fps = {t: fingerprint(d) for t, d in tables.items()}
    results = []
    seen = set()
    for a in fps:
        for b in fps:
            if a >= b:
                continue
            key = (a, b)
            if key in seen:
                continue
            seen.add(key)
            fa, fb = fps[a], fps[b]
            if not fa or not fb:
                continue
            # Jaccard on multiset
            sa, sb = list(fa), list(fb)
            common = sum((sa.count(t) for t in set(sa) & set(fb)))
            union  = len(sa) + len(sb) - common
            score  = common / union if union else 0
            if score >= threshold and len(fa) > 2:
                results.append({
                    "type":      "structural_duplicate",
                    "table_a":   a,
                    "table_b":   b,
                    "score":     round(score, 3),
                    "confidence": "medium",
                    "reason":    f"Column type fingerprints match {round(score*100)}% — possible duplicate/partition tables",
                })
    return sorted(results, key=lambda x: -x["score"])


# ─── Main runner ──────────────────────────────────────────────

def run_all(schema: dict) -> dict:
    """Run all three passes and return consolidated results."""
    sim_tables  = find_similar_tables(schema)
    inferred_fk = find_similar_columns(schema)
    val_overlap = find_value_overlaps(schema)
    structural  = find_structural_duplicates(schema)

    # Merge inferred FKs: prefer value_overlap over name similarity
    fk_map: dict[tuple, dict] = {}
    for r in inferred_fk:
        k = (r["from_table"], r["from_col"])
        fk_map[k] = r
    for r in val_overlap:
        k = (r["from_table"], r["from_col"])
        if k not in fk_map or r["score"] > fk_map[k]["score"]:
            fk_map[k] = r

    merged_fks = sorted(fk_map.values(), key=lambda x: -x["score"])

    return {
        "inferred_fks":          merged_fks,
        "similar_tables":        sim_tables,
        "structural_duplicates": structural,
        "summary": {
            "inferred_fk_count":   len(merged_fks),
            "similar_table_count": len(sim_tables),
            "structural_dup_count":len(structural),
        },
    }
