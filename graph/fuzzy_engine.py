"""
DB Blueprint v2 — Fuzzy Analytic Engine
========================================
Name-blind relation discovery via data fingerprinting.

Three signal layers:
  1. Data signals     — value overlap, cardinality, range, null density
  2. Pattern signals  — format fingerprint, regex clusters, prefix/suffix, length dist
  3. Structural       — type vector, row-count magnitude, co-occurrence

All signals fuse into a weighted composite score stored as a
Neo4j FUZZY_RELATES_TO edge (or returned standalone if Neo4j is absent).
"""

from __future__ import annotations

import re
import math
import hashlib
from collections import Counter
from typing import Any


# ─── Signal weights (tunable) ────────────────────────────────
WEIGHTS = {
    "value_overlap":       0.40,   # strongest signal — actual data matches
    "format_fingerprint":  0.20,   # UUID/date/phone format matches
    "cardinality_match":   0.18,   # both near-unique or both low-cardinality
    "range_overlap":       0.10,   # numeric min/max range similarity
    "null_density_match":  0.05,   # similar null % suggests same usage
    "length_distribution": 0.05,   # similar string length profile
    "prefix_cluster":      0.02,   # shared value prefix (ORD-, TXN-)
}

# ─── Format patterns ─────────────────────────────────────────
FORMAT_PATTERNS = [
    ("uuid",        re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)),
    ("uuid_nodash", re.compile(r"^[0-9a-f]{32}$", re.I)),
    ("email",       re.compile(r"^[^@]+@[^@]+\.[^@]+$")),
    ("phone_id",    re.compile(r"^\+?[0-9]{8,15}$")),
    ("date_iso",    re.compile(r"^\d{4}-\d{2}-\d{2}$")),
    ("date_compact",re.compile(r"^\d{8}$")),
    ("integer_id",  re.compile(r"^\d{1,12}$")),
    ("alpha_code",  re.compile(r"^[A-Z]{2,6}$")),
    ("prefixed_id", re.compile(r"^[A-Z]{2,6}[-_]\d+$")),
    ("hex_hash",    re.compile(r"^[0-9a-f]{16,64}$", re.I)),
    ("ip_address",  re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")),
]


def _safe_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _extract_prefix(v: str, min_len: int = 2, max_len: int = 6) -> str | None:
    """Extract leading alpha prefix like 'ORD' from 'ORD-001234'."""
    m = re.match(r"^([A-Za-z]{%d,%d})[-_\s]" % (min_len, max_len), v)
    return m.group(1).upper() if m else None


# ─── Column fingerprint ───────────────────────────────────────

def fingerprint_column(col_name: str, col_type: str,
                        sample_values: list[Any]) -> dict:
    """
    Compute a rich fingerprint for one column from its sample values.
    Returns a dict of signals used for pairwise comparison.
    """
    raw = [_safe_str(v) for v in sample_values]
    vals = [v for v in raw if v is not None]

    total      = len(sample_values)
    non_null   = len(vals)
    null_pct   = 1.0 - (non_null / total) if total else 0.0

    if not vals:
        return {
            "col_name":    col_name,
            "col_type":    col_type,
            "null_pct":    1.0,
            "unique_pct":  0.0,
            "value_set":   set(),
            "format":      "empty",
            "prefix_set":  set(),
            "len_p50":     0,
            "len_p95":     0,
            "numeric_min": None,
            "numeric_max": None,
            "numeric_mean":None,
        }

    # Uniqueness
    unique_vals = set(vals)
    unique_pct  = len(unique_vals) / non_null

    # Format detection — vote on most common format
    format_votes: Counter = Counter()
    for v in vals[:100]:
        for fname, pattern in FORMAT_PATTERNS:
            if pattern.match(v):
                format_votes[fname] += 1
                break
        else:
            format_votes["freetext"] += 1
    dominant_format = format_votes.most_common(1)[0][0]
    format_confidence = format_votes.most_common(1)[0][1] / min(len(vals), 100)

    # Prefix clusters
    prefixes = {p for v in vals[:200] if (p := _extract_prefix(v))}

    # Length distribution
    lengths = sorted(len(v) for v in vals)
    n = len(lengths)
    len_p50 = lengths[n // 2] if lengths else 0
    len_p95 = lengths[int(n * 0.95)] if lengths else 0

    # Numeric stats
    numeric_vals = []
    for v in vals[:200]:
        try:
            numeric_vals.append(float(v.replace(",", "").replace(" ", "")))
        except (ValueError, AttributeError):
            pass

    num_min = min(numeric_vals) if numeric_vals else None
    num_max = max(numeric_vals) if numeric_vals else None
    num_mean = sum(numeric_vals) / len(numeric_vals) if numeric_vals else None

    return {
        "col_name":          col_name,
        "col_type":          col_type,
        "null_pct":          round(null_pct, 3),
        "unique_pct":        round(unique_pct, 3),
        "value_set":         unique_vals,
        "format":            dominant_format,
        "format_confidence": round(format_confidence, 3),
        "prefix_set":        prefixes,
        "len_p50":           len_p50,
        "len_p95":           len_p95,
        "numeric_min":       num_min,
        "numeric_max":       num_max,
        "numeric_mean":      num_mean,
        "sample_count":      non_null,
    }


# ─── Pairwise column scoring ──────────────────────────────────

def score_column_pair(fp_a: dict, fp_b: dict) -> dict:
    """
    Score how likely two columns are to represent the same domain,
    purely from data fingerprints — no names involved.
    Returns individual signal scores + weighted composite.
    """
    signals: dict[str, float] = {}

    # 1. Value overlap (Jaccard on sampled value sets)
    va, vb = fp_a["value_set"], fp_b["value_set"]
    if va and vb:
        inter = len(va & vb)
        union = len(va | vb)
        signals["value_overlap"] = inter / union if union else 0.0
    else:
        signals["value_overlap"] = 0.0

    # 2. Format fingerprint match
    if fp_a["format"] == fp_b["format"] and fp_a["format"] != "freetext":
        # Weight by confidence of both
        signals["format_fingerprint"] = (
            fp_a.get("format_confidence", 0.5) *
            fp_b.get("format_confidence", 0.5)
        ) ** 0.5
    else:
        signals["format_fingerprint"] = 0.0

    # 3. Cardinality match — both near-unique (FK candidate) or both low-card (lookup)
    ua, ub = fp_a["unique_pct"], fp_b["unique_pct"]
    cardinality_diff = abs(ua - ub)
    # High match when both are near 1.0 (unique IDs) or both near 0 (status codes)
    if ua > 0.8 and ub > 0.8:
        signals["cardinality_match"] = 1.0 - cardinality_diff
    elif ua < 0.1 and ub < 0.1:
        signals["cardinality_match"] = 1.0 - cardinality_diff * 5
    else:
        signals["cardinality_match"] = max(0.0, 1.0 - cardinality_diff * 2)

    # 4. Numeric range overlap
    min_a, max_a = fp_a.get("numeric_min"), fp_a.get("numeric_max")
    min_b, max_b = fp_b.get("numeric_min"), fp_b.get("numeric_max")
    if all(v is not None for v in [min_a, max_a, min_b, max_b]) and max_a > min_a and max_b > min_b:
        overlap_start = max(min_a, min_b)
        overlap_end   = min(max_a, max_b)
        if overlap_end > overlap_start:
            range_a = max_a - min_a
            range_b = max_b - min_b
            overlap  = overlap_end - overlap_start
            signals["range_overlap"] = overlap / max(range_a, range_b)
        else:
            signals["range_overlap"] = 0.0
    else:
        signals["range_overlap"] = 0.5   # neutral when non-numeric

    # 5. Null density similarity
    null_diff = abs(fp_a["null_pct"] - fp_b["null_pct"])
    signals["null_density_match"] = max(0.0, 1.0 - null_diff * 4)

    # 6. Length distribution similarity
    len_diff_p50 = abs(fp_a["len_p50"] - fp_b["len_p50"])
    len_diff_p95 = abs(fp_a["len_p95"] - fp_b["len_p95"])
    max_len = max(fp_a["len_p50"], fp_b["len_p50"], fp_a["len_p95"], fp_b["len_p95"], 1)
    signals["length_distribution"] = max(0.0, 1.0 - (len_diff_p50 + len_diff_p95) / (max_len * 2))

    # 7. Prefix cluster overlap
    pa, pb = fp_a["prefix_set"], fp_b["prefix_set"]
    if pa and pb:
        prefix_inter = len(pa & pb) / len(pa | pb)
        signals["prefix_cluster"] = prefix_inter
    else:
        signals["prefix_cluster"] = 0.5   # neutral

    # Weighted composite
    composite = sum(WEIGHTS[k] * signals[k] for k in WEIGHTS)
    strong_factual = max(signals["value_overlap"], signals["format_fingerprint"], signals["cardinality_match"])
    if strong_factual < 0.45:
        composite *= 0.55
    elif signals["value_overlap"] < 0.15 and signals["format_fingerprint"] < 0.2:
        composite *= 0.7

    # Confidence band
    if composite >= 0.80:
        confidence = "high"
    elif composite >= 0.55:
        confidence = "medium"
    elif composite >= 0.35:
        confidence = "low"
    else:
        confidence = "none"

    return {
        "composite_score": round(composite, 4),
        "confidence":      confidence,
        "signals":         {k: round(v, 4) for k, v in signals.items()},
        "dominant_signal": max(signals, key=signals.get),
    }


# ─── Schema-level fuzzy analysis ─────────────────────────────

def _extract_samples_from_schema(schema: dict) -> dict[tuple, list]:
    """
    Returns {(table_name, col_name): [sample_values]} from schema dict.
    """
    result = {}
    for tname, tdata in schema.get("tables", {}).items():
        samples = tdata.get("samples", [])
        for col in tdata.get("columns", []):
            cname = col["column_name"]
            values = [row.get(cname) for row in samples if cname in row]
            if values:
                result[(tname, cname)] = values
    return result


def run_fuzzy_analysis(schema: dict,
                       min_score: float = 0.55,
                       skip_same_table: bool = True,
                       skip_explicit_fks: bool = True) -> dict:
    """
    Full schema-level fuzzy analysis.
    Returns discovered relations sorted by composite score.
    """
    # Build explicit FK set to optionally skip
    explicit_fk_set = set()
    if skip_explicit_fks:
        for fk in schema.get("explicit_fks", []):
            explicit_fk_set.add((fk["from_table"], fk["from_col"]))

    # Build fingerprints for every column that has sample data
    sample_map = _extract_samples_from_schema(schema)
    tables_data = schema.get("tables", {})

    fingerprints: dict[tuple, dict] = {}
    for (tname, cname), values in sample_map.items():
        col_meta = next(
            (c for c in tables_data[tname]["columns"] if c["column_name"] == cname),
            {}
        )
        fp = fingerprint_column(cname, col_meta.get("data_type", ""), values)
        fingerprints[(tname, cname)] = fp

    all_keys = list(fingerprints.keys())
    relations = []
    seen = set()

    for i, key_a in enumerate(all_keys):
        tname_a, cname_a = key_a
        fp_a = fingerprints[key_a]

        # Skip if too few samples to be meaningful
        if fp_a.get("sample_count", 0) < 3:
            continue

        for key_b in all_keys[i+1:]:
            tname_b, cname_b = key_b
            fp_b = fingerprints[key_b]

            if fp_b.get("sample_count", 0) < 3:
                continue
            if skip_same_table and tname_a == tname_b:
                continue
            if key_a in explicit_fk_set or key_b in explicit_fk_set:
                continue

            # Dedup: only keep best score per (col_a, col_b) pair
            pair_key = tuple(sorted([key_a, key_b]))
            if pair_key in seen:
                continue
            seen.add(pair_key)

            result = score_column_pair(fp_a, fp_b)
            if result["composite_score"] < min_score:
                continue

            # Build human-readable evidence summary
            sigs = result["signals"]
            evidence_parts = []
            if sigs.get("value_overlap", 0) > 0.5:
                pct = round(sigs["value_overlap"] * 100)
                evidence_parts.append(f"{pct}% value overlap")
            if sigs.get("format_fingerprint", 0) > 0.6:
                evidence_parts.append(f"matching format ({fp_a['format']})")
            if sigs.get("cardinality_match", 0) > 0.7:
                evidence_parts.append("similar cardinality")
            if sigs.get("prefix_cluster", 0) > 0.6 and fp_a["prefix_set"]:
                evidence_parts.append(f"shared prefix ({list(fp_a['prefix_set'])[:2]})")
            if sigs.get("range_overlap", 0) > 0.7:
                evidence_parts.append("overlapping numeric range")

            evidence = " · ".join(evidence_parts) if evidence_parts else "statistical similarity"

            relations.append({
                "from_table":      tname_a,
                "from_col":        cname_a,
                "to_table":        tname_b,
                "to_col":          cname_b,
                "composite_score": result["composite_score"],
                "confidence":      result["confidence"],
                "dominant_signal": result["dominant_signal"],
                "evidence":        evidence,
                "signals":         result["signals"],
                # Fingerprint summary for display
                "from_format":     fp_a["format"],
                "to_format":       fp_b["format"],
                "from_unique_pct": fp_a["unique_pct"],
                "to_unique_pct":   fp_b["unique_pct"],
            })

    # Sort by score descending
    relations.sort(key=lambda x: -x["composite_score"])

    # Table-level duplicate detection via column-vector similarity
    table_duplicates = _find_table_duplicates_by_data(schema, sample_map, fingerprints)

    return {
        "relations":       relations,
        "table_duplicates":table_duplicates,
        "summary": {
            "columns_fingerprinted": len(fingerprints),
            "pairs_evaluated":       len(seen),
            "relations_found":       len(relations),
            "high_confidence":       sum(1 for r in relations if r["confidence"] == "high"),
            "medium_confidence":     sum(1 for r in relations if r["confidence"] == "medium"),
            "low_confidence":        sum(1 for r in relations if r["confidence"] == "low"),
        },
    }


def _find_table_duplicates_by_data(schema: dict,
                                   sample_map: dict,
                                   fingerprints: dict) -> list[dict]:
    """
    Detect tables that look like duplicates/partitions based on
    their column data profiles — not their names.
    """
    tables = schema.get("tables", {})

    # Build per-table format vector: {format: count}
    table_vectors: dict[str, dict] = {}
    for tname in tables:
        vec: dict[str, int] = Counter()
        for (t, c), fp in fingerprints.items():
            if t == tname:
                vec[fp["format"]] += 1
        if vec:
            table_vectors[tname] = dict(vec)

    duplicates = []
    tnames = list(table_vectors.keys())
    seen = set()

    for i, ta in enumerate(tnames):
        for tb in tnames[i+1:]:
            pair = tuple(sorted([ta, tb]))
            if pair in seen:
                continue
            seen.add(pair)

            va, vb = table_vectors[ta], table_vectors[tb]
            all_keys = set(va) | set(vb)
            if not all_keys:
                continue

            # Cosine similarity of format vectors
            dot = sum(va.get(k, 0) * vb.get(k, 0) for k in all_keys)
            mag_a = math.sqrt(sum(v**2 for v in va.values()))
            mag_b = math.sqrt(sum(v**2 for v in vb.values()))
            if mag_a == 0 or mag_b == 0:
                continue

            cosine = dot / (mag_a * mag_b)

            # Also compare row count magnitude class
            rc_a = tables[ta].get("row_count", 0)
            rc_b = tables[tb].get("row_count", 0)

            def magnitude(n):
                if n < 100: return "tiny"
                if n < 10_000: return "small"
                if n < 1_000_000: return "medium"
                return "large"

            mag_match = magnitude(rc_a) == magnitude(rc_b)

            score = cosine * (1.1 if mag_match else 0.9)
            score = min(score, 1.0)

            if score >= 0.75:
                duplicates.append({
                    "table_a":       ta,
                    "table_b":       tb,
                    "score":         round(score, 4),
                    "confidence":    "high" if score >= 0.90 else "medium",
                    "format_cosine": round(cosine, 4),
                    "row_mag_match": mag_match,
                    "reason":        f"Data profile similarity {round(score*100)}% — possible duplicate, partition, or archive table",
                })

    duplicates.sort(key=lambda x: -x["score"])
    return duplicates
