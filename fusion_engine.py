from __future__ import annotations

from collections import defaultdict
from typing import Any


def _conf(score: float, approved: bool = False, explicit: bool = False) -> str:
    if approved:
        return 'approved'
    if explicit or score >= 0.92:
        return 'high'
    if score >= 0.72:
        return 'medium'
    return 'low'


def _norm_rel(rel: dict[str, Any], defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    d = dict(defaults or {})
    d.update(rel or {})
    return {
        'from_table': str(d.get('from_table') or '').strip(),
        'from_col': str(d.get('from_col') or d.get('from_column') or '').strip(),
        'to_table': str(d.get('to_table') or '').strip(),
        'to_col': str(d.get('to_col') or d.get('to_column') or 'id').strip() or 'id',
        'score': float(d.get('score') or d.get('composite_score') or 0.0),
        'confidence': str(d.get('confidence') or 'medium'),
        'source': str(d.get('source') or ''),
        'signal': str(d.get('signal') or d.get('dominant_signal') or ''),
        'evidence': str(d.get('evidence') or d.get('reason') or '').strip(),
        'parser_mode': str(d.get('parser_mode') or ''),
        'rel_type': str(d.get('rel_type') or ''),
        'notes': str(d.get('notes') or '').strip(),
        'id': d.get('id'),
    }


def _add(bucket: dict, relation: dict, kind: str, strength: float, note: str = '') -> None:
    bucket['signals'].append(kind)
    bucket['weighted_score'] += strength
    bucket['support'].append({
        'kind': kind,
        'strength': round(strength, 3),
        'score': round(relation.get('score', 0.0), 3),
        'confidence': relation.get('confidence', ''),
        'parser_mode': relation.get('parser_mode', ''),
        'evidence': relation.get('evidence', '') or note,
    })
    if relation.get('evidence'):
        bucket['evidence'].append(relation['evidence'])


def fuse_relations(
    schema: dict,
    dedup_results: dict | None = None,
    fuzzy_results: dict | None = None,
    source_diff: dict | None = None,
    approved_relations: list[dict] | None = None,
) -> dict:
    dedup_results = dedup_results or {}
    fuzzy_results = fuzzy_results or {}
    source_diff = source_diff or {}
    approved_relations = approved_relations or []

    buckets: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    by_origin: dict[tuple[str, str], list[tuple[str, str, str, str]]] = defaultdict(list)

    def ensure(rel: dict[str, Any]) -> dict[str, Any]:
        key = (rel['from_table'], rel['from_col'], rel['to_table'], rel['to_col'])
        if key not in buckets:
            buckets[key] = {
                'from_table': rel['from_table'], 'from_col': rel['from_col'],
                'to_table': rel['to_table'], 'to_col': rel['to_col'],
                'weighted_score': 0.0, 'signals': [], 'support': [], 'evidence': [],
                'approved': False, 'explicit': False, 'source_confirmed': False,
            }
            by_origin[(rel['from_table'], rel['from_col'])].append(key)
        return buckets[key]

    for fk in schema.get('explicit_fks', []) or []:
        rel = _norm_rel(fk, {'score': 1.0, 'confidence': 'explicit', 'source': 'explicit_fk', 'signal': 'foreign_key', 'evidence': 'Explicit FK metadata'})
        bucket = ensure(rel)
        bucket['explicit'] = True
        _add(bucket, rel, 'explicit_fk', 1.00)

    for rel0 in dedup_results.get('inferred_fks', []) or []:
        rel = _norm_rel(rel0, {'source': 'dedup', 'signal': 'name_or_value_inference'})
        bucket = ensure(rel)
        prov = rel0.get('provenance') or []
        has_value = 'value_overlap' in prov or 'overlap' in rel.get('signal', '') or rel.get('rel_type') == 'value_overlap'
        has_name = 'name_similarity' in prov or 'name' in rel.get('signal', '') or 'stem' in rel.get('evidence', '').lower()
        if has_name:
            _add(bucket, rel, 'dedup_name', min(0.32, 0.12 + rel['score'] * 0.22))
        if has_value:
            _add(bucket, rel, 'value_overlap', min(0.42, 0.16 + rel['score'] * 0.28))
        if not has_name and not has_value:
            _add(bucket, rel, 'dedup', min(0.28, 0.10 + rel['score'] * 0.18))

    for rel0 in fuzzy_results.get('relations', []) or []:
        rel = _norm_rel(rel0, {'source': 'fuzzy_fingerprint'})
        bucket = ensure(rel)
        strength = min(0.38, 0.08 + rel['score'] * 0.30)
        _add(bucket, rel, 'fuzzy', strength)

    for rel0 in (source_diff.get('source_relations') or []):
        rel = _norm_rel(rel0, {'source': 'orm_model', 'signal': 'source_code'})
        bucket = ensure(rel)
        bucket['source_confirmed'] = True
        parser_mode = rel.get('parser_mode') or 'heuristic'
        bonus = {'ast': 0.46, 'structured': 0.44, 'heuristic': 0.30}.get(parser_mode, 0.30)
        _add(bucket, rel, 'source_code', bonus, note=f'source parser={parser_mode}')

    for rel0 in approved_relations:
        rel = _norm_rel(rel0, {'source': rel0.get('source') or 'user_approved', 'signal': rel0.get('signal') or 'human_review'})
        bucket = ensure(rel)
        bucket['approved'] = True
        if rel.get('source') == 'orm_model' or rel.get('signal') == 'orm_model':
            bucket['source_confirmed'] = True
        _add(bucket, rel, 'approved', 1.05, note='Curated reviewer truth')

    fused = []
    for key, b in buckets.items():
        score = min(1.0, b['weighted_score'])
        confidence = _conf(score, approved=b['approved'], explicit=b['explicit'])
        signal_set = list(dict.fromkeys(b['signals']))
        dominant = signal_set[0] if signal_set else 'unknown'
        fused.append({
            'from_table': b['from_table'], 'from_col': b['from_col'],
            'to_table': b['to_table'], 'to_col': b['to_col'],
            'fusion_score': round(score, 3),
            'confidence': confidence,
            'approved': b['approved'],
            'explicit': b['explicit'],
            'source_confirmed': b['source_confirmed'],
            'provenance': signal_set,
            'dominant_signal': dominant,
            'evidence': '; '.join(dict.fromkeys([e for e in b['evidence'] if e]))[:1000],
            'support': b['support'],
            'rel_type': 'fused_relation',
        })

    fused.sort(key=lambda x: (-x['fusion_score'], x['from_table'], x['from_col'], x['to_table']))

    disagreements = []
    for origin, keys in by_origin.items():
        candidates = [next(f for f in fused if (f['from_table'], f['from_col'], f['to_table'], f['to_col']) == key) for key in keys]
        if len(candidates) < 2:
            continue
        candidates = sorted(candidates, key=lambda x: (-x['fusion_score'], x['to_table'], x['to_col']))
        best, second = candidates[0], candidates[1]
        if second['fusion_score'] >= max(0.45, best['fusion_score'] - 0.12):
            disagreements.append({
                'from_table': origin[0], 'from_col': origin[1],
                'best_target': f"{best['to_table']}.{best['to_col']}",
                'best_score': best['fusion_score'],
                'competing_target': f"{second['to_table']}.{second['to_col']}",
                'competing_score': second['fusion_score'],
                'reason': 'Multiple plausible targets remain close after fusion',
            })

    summary = {
        'fused_relation_count': len(fused),
        'high_confidence_count': sum(1 for r in fused if r['confidence'] in {'approved', 'high'}),
        'source_confirmed_count': sum(1 for r in fused if r['source_confirmed']),
        'explicit_count': sum(1 for r in fused if r['explicit']),
        'approved_count': sum(1 for r in fused if r['approved']),
        'disagreement_count': len(disagreements),
    }
    return {'relations': fused, 'disagreements': disagreements, 'summary': summary}
