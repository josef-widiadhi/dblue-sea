"""
DB Blueprint v2 — Similarity & Deduplication Engine
"""

from __future__ import annotations
import re

DOMAIN_LEXICON = {
    'banking': {
        'txn': 'transaction', 'tx': 'transaction', 'acct': 'account', 'acc': 'account', 'cust': 'customer', 'amt': 'amount',
        'bal': 'balance', 'br': 'branch', 'src': 'source', 'dst': 'destination', 'ts': 'timestamp', 'stat': 'status',
        'gl': 'ledger', 'ccy': 'currency', 'curr': 'currency', 'clr': 'cleared'
    },
    'healthcare': {
        'pt': 'patient', 'pat': 'patient', 'adm': 'admission', 'dx': 'diagnosis', 'rx': 'prescription', 'mrn': 'medical_record',
        'dob': 'birth_date', 'dr': 'doctor', 'prov': 'provider'
    },
    'logistics': {
        'awb': 'air_waybill', 'pod': 'proof_of_delivery', 'wh': 'warehouse', 'eta': 'estimated_arrival', 'etd': 'estimated_departure',
        'cons': 'consignment', 'dest': 'destination', 'orig': 'origin', 'trk': 'tracking'
    },
    'ecommerce': {
        'ord': 'order', 'sku': 'stock_keeping_unit', 'qty': 'quantity', 'inv': 'invoice', 'pmt': 'payment', 'cust': 'customer'
    },
    'manufacturing': {
        'bom': 'bill_of_materials', 'wip': 'work_in_progress', 'wo': 'work_order', 'po': 'purchase_order', 'fg': 'finished_goods'
    },
    'telco': {
        'msisdn': 'subscriber_number', 'cdr': 'call_detail_record', 'subs': 'subscriber', 'apn': 'access_point_name'
    },
    'insurance': {
        'pol': 'policy', 'prm': 'premium', 'clm': 'claim', 'cov': 'coverage', 'ben': 'beneficiary'
    },
}
GENERIC_ALIASES = {
    'id': 'identifier', 'no': 'number', 'cd': 'code', 'ref': 'reference', 'dt': 'date', 'ts': 'timestamp'
}


def merge_lexicons(*dicts: dict | None) -> dict:
    merged = {}
    for d in dicts:
        for k, v in (d or {}).items():
            kk = str(k).strip().lower()
            vv = str(v).strip().lower()
            if kk and vv:
                merged[kk] = vv
    return merged


def _ctx(ctx: dict | None) -> dict:
    ctx = ctx or {}
    industry = (ctx.get('industry') or 'other').lower()
    lex = merge_lexicons(GENERIC_ALIASES, DOMAIN_LEXICON.get(industry, {}), ctx.get('dictionary'), ctx.get('dictionary_json'))
    for hint in ctx.get('hints', []) or []:
        if '=' in str(hint):
            k, v = hint.split('=', 1)
            lex[k.strip().lower()] = v.strip().lower()
    return {'industry': industry, 'lexicon': lex, 'subdomain': ctx.get('subdomain', ''), 'region': ctx.get('region', '')}


def _expand_token(token: str, ctx: dict | None) -> str:
    c = _ctx(ctx)
    return c['lexicon'].get(token.lower(), token.lower())


def _normalise(name: str, ctx: dict | None = None) -> str:
    n = name.lower().strip()
    n = re.sub(r'[_\-\s]+', ' ', n).strip()
    n = re.sub(r'^(tbl|t|m|v|tb|ref|mst|trx|tmp|temp)\s+', '', n)
    toks = [_expand_token(t, ctx) for t in n.split() if t]
    n = ' '.join(toks)
    if n.endswith('ies'):
        n = n[:-3] + 'y'
    elif n.endswith('ses') or n.endswith('xes'):
        n = n[:-2]
    elif n.endswith('s') and len(n) > 3:
        n = n[:-1]
    return n


def _token_set(name: str, ctx: dict | None = None) -> set[str]:
    tokens = re.split(r'[_\-\s]+', name.lower())
    return {_expand_token(t, ctx) for t in tokens if len(t) > 1}


def _similarity(a: str, b: str, ctx: dict | None = None) -> float:
    try:
        from rapidfuzz import fuzz
        seq = fuzz.ratio(a.lower(), b.lower()) / 100.0
        norm_sim = fuzz.ratio(_normalise(a, ctx), _normalise(b, ctx)) / 100.0
    except ImportError:
        from difflib import SequenceMatcher
        seq = SequenceMatcher(None, a.lower(), b.lower()).ratio()
        norm_sim = SequenceMatcher(None, _normalise(a, ctx), _normalise(b, ctx)).ratio()
    ta, tb = _token_set(a, ctx), _token_set(b, ctx)
    if not ta and not tb:
        jaccard = 1.0
    elif not ta or not tb:
        jaccard = 0.0
    else:
        jaccard = len(ta & tb) / len(ta | tb)
    return max(seq, jaccard, norm_sim)


def infer_dictionary_candidates(schema: dict, ctx: dict | None = None) -> list[dict]:
    c = _ctx(ctx)
    lex = c['lexicon']
    found = {}
    for tname, tdata in (schema.get('tables', {}) or {}).items():
        tokens = re.split(r'[_\-\s]+', str(tname).lower())
        for col in tdata.get('columns', []) or []:
            tokens.extend(re.split(r'[_\-\s]+', str(col.get('column_name', '')).lower()))
        for tok in tokens:
            tok = tok.strip().lower()
            if len(tok) < 2 or tok not in lex:
                continue
            row = found.setdefault(tok, {'token': tok, 'meaning': lex[tok], 'source': 'domain_lexicon', 'hits': 0, 'examples': []})
            row['hits'] += 1
            if tname not in row['examples'] and len(row['examples']) < 6:
                row['examples'].append(tname)
    return sorted(found.values(), key=lambda x: (-x['hits'], x['token']))


def find_similar_tables(schema: dict, threshold: float = 0.72, context: dict | None = None) -> list[dict]:
    tables = list(schema.get('tables', {}).keys())
    results = []
    for i, a in enumerate(tables):
        for b in tables[i + 1:]:
            score = _similarity(a, b, context)
            if score >= threshold:
                results.append({
                    'type': 'similar_tables', 'table_a': a, 'table_b': b, 'score': round(score, 3),
                    'confidence': 'high' if score >= 0.9 else 'medium',
                    'reason': f"Domain-grounded name similarity {round(score * 100)}% — possible duplicate or alias",
                })
    return sorted(results, key=lambda x: -x['score'])


def find_similar_columns(schema: dict, threshold: float = 0.80, context: dict | None = None) -> list[dict]:
    tables = schema.get('tables', {})
    pk_index = {tname: [c['column_name'] for c in tdata.get('columns', []) if c.get('is_pk')] for tname, tdata in tables.items()}
    pk_index = {k: v for k, v in pk_index.items() if v}
    results = []
    fk_patterns = re.compile(r'(.+?)(_id|_cd|_no|_key|_code|_ref|Id|Cd|No|Key)$', re.IGNORECASE)
    for src_table, tdata in tables.items():
        for col in tdata.get('columns', []):
            cname = col['column_name']
            m = fk_patterns.match(cname)
            if not m:
                continue
            stem = _normalise(m.group(1), context)
            for ref_table, pk_cols in pk_index.items():
                if ref_table == src_table:
                    continue
                for pk_col in pk_cols:
                    score = _similarity(stem, ref_table, context)
                    if score >= threshold:
                        results.append({
                            'type': 'inferred_fk', 'from_table': src_table, 'from_col': cname,
                            'to_table': ref_table, 'to_col': pk_col, 'score': round(score, 3),
                            'confidence': 'high' if score >= 0.92 else 'medium',
                            'reason': f"Domain-grounded stem '{stem}' matches table '{ref_table}' ({round(score * 100)}% similarity)",
                            'provenance': ['name_similarity', 'domain_grounding'],
                        })
    best = {}
    for r in results:
        key = (r['from_table'], r['from_col'])
        if key not in best or r['score'] > best[key]['score']:
            best[key] = r
    return sorted(best.values(), key=lambda x: -x['score'])


def find_value_overlaps(schema: dict, min_overlap: float = 0.55) -> list[dict]:
    tables = schema.get('tables', {})
    results = []
    pk_values = {}
    for tname, tdata in tables.items():
        for col in tdata.get('columns', []):
            if col.get('is_pk'):
                vals = {str(row.get(col['column_name'], '')) for row in tdata.get('samples', []) if row.get(col['column_name'])}
                if vals:
                    pk_values[(tname, col['column_name'])] = vals
    for src_table, tdata in tables.items():
        for col in tdata.get('columns', []):
            cname = col['column_name']
            if col.get('is_pk'):
                continue
            src_vals = {str(row.get(cname, '')) for row in tdata.get('samples', []) if row.get(cname)}
            if not src_vals:
                continue
            for (ref_table, ref_col), ref_vals in pk_values.items():
                if ref_table == src_table or not ref_vals:
                    continue
                inter = len(src_vals & ref_vals)
                overlap = inter / max(len(src_vals), 1)
                coverage = inter / max(len(ref_vals), 1)
                if overlap >= min_overlap and inter >= 3:
                    results.append({
                        'type': 'value_overlap', 'from_table': src_table, 'from_col': cname, 'to_table': ref_table, 'to_col': ref_col,
                        'overlap_pct': round(overlap * 100, 1), 'score': round(overlap, 3),
                        'confidence': 'high' if overlap >= 0.7 else 'medium',
                        'reason': f"{round(overlap * 100)}% source overlap and {round(coverage * 100)}% target coverage between sample sets",
                        'provenance': ['value_overlap'],
                    })
    return sorted(results, key=lambda x: -x['score'])


def find_structural_duplicates(schema: dict, threshold: float = 0.93) -> list[dict]:
    tables = schema.get('tables', {})

    def fingerprint(tdata: dict) -> tuple:
        cols = tdata.get('columns', [])
        return (
            tuple(sorted((c.get('data_type') or '').lower() for c in cols)),
            len(cols),
            sum(1 for c in cols if c.get('is_pk')),
        )

    fps = {t: fingerprint(d) for t, d in tables.items()}
    results = []
    keys = list(fps)
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            fa, fb = fps[a], fps[b]
            if not fa or not fb:
                continue
            types_a, cols_a, pk_a = fa
            types_b, cols_b, pk_b = fb
            if abs(cols_a - cols_b) > max(1, int(max(cols_a, cols_b) * 0.15)):
                continue
            common = sum(list(types_a).count(t) for t in set(types_a) & set(types_b))
            union = len(types_a) + len(types_b) - common
            type_score = common / union if union else 0
            col_score = 1.0 - (abs(cols_a - cols_b) / max(cols_a, cols_b, 1))
            pk_score = 1.0 if pk_a == pk_b else 0.5
            score = (type_score * 0.75) + (col_score * 0.2) + (pk_score * 0.05)
            if score >= threshold and cols_a > 2 and cols_b > 2:
                results.append({
                    'type': 'structural_duplicate', 'table_a': a, 'table_b': b, 'score': round(score, 3),
                    'confidence': 'medium', 'reason': f"Structural fingerprints match {round(score * 100)}% with similar column counts — possible duplicate/partition tables",
                })
    return sorted(results, key=lambda x: -x['score'])


def run_all(schema: dict, context: dict | None = None) -> dict:
    ctx = _ctx(context)
    sim_tables = find_similar_tables(schema, context=ctx)
    inferred_fk = find_similar_columns(schema, context=ctx)
    val_overlap = find_value_overlaps(schema)
    structural = find_structural_duplicates(schema)
    fk_map = {(r['from_table'], r['from_col']): r for r in inferred_fk}
    for r in val_overlap:
        k = (r['from_table'], r['from_col'])
        if k not in fk_map or r['score'] > fk_map[k]['score']:
            if k in fk_map:
                r['provenance'] = sorted(set((fk_map[k].get('provenance') or []) + ['value_overlap']))
                r['reason'] += '; outranks name-based guess'
            fk_map[k] = r
    merged_fks = sorted(fk_map.values(), key=lambda x: -x['score'])
    dictionary_found = infer_dictionary_candidates(schema, ctx)
    return {
        'domain_context': {'industry': ctx['industry'], 'lexicon': ctx['lexicon'], 'subdomain': ctx['subdomain'], 'region': ctx['region']},
        'inferred_fks': merged_fks,
        'similar_tables': sim_tables,
        'structural_duplicates': structural,
        'value_overlaps': val_overlap,
        'dictionary_found': dictionary_found,
        'effective_dictionary': ctx['lexicon'],
        'summary': {
            'inferred_fk_count': len(merged_fks), 'similar_table_count': len(sim_tables), 'structural_dup_count': len(structural),
            'value_overlap_count': len(val_overlap), 'dictionary_found_count': len(dictionary_found), 'domain_terms': len(ctx['lexicon'])
        },
    }
