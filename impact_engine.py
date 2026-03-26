from __future__ import annotations

from collections import defaultdict
from typing import Any

KIND_WEIGHT = {
    'FrontendPage': 1.8,
    'FrontendComponent': 1.2,
    'APIRoute': 2.0,
    'BackendFunction': 1.5,
    'ORMModel': 1.1,
    'Table': 2.3,
}


def _node_kind(nid: str) -> str:
    return (nid.split(':', 1)[0] if ':' in nid else '').strip()


def _node_name(nid: str) -> str:
    return (nid.split(':', 1)[1] if ':' in nid else nid).strip()


def score_cartography(cart: dict[str, Any]) -> dict[str, Any]:
    visual = cart.get('visual') or {}
    nodes = visual.get('nodes') or []
    edges = visual.get('edges') or []
    node_by_id = {n['id']: n for n in nodes}
    outgoing = defaultdict(set)
    incoming = defaultdict(set)
    for e in edges:
        outgoing[e['from']].add(e['to'])
        incoming[e['to']].add(e['from'])

    scored = []
    for nid, n in node_by_id.items():
        kind = n.get('kind') or _node_kind(nid)
        indeg = len(incoming[nid])
        outdeg = len(outgoing[nid])
        neighbor_kinds = defaultdict(int)
        for x in incoming[nid] | outgoing[nid]:
            neighbor_kinds[_node_kind(x)] += 1
        reach2 = set()
        for x in incoming[nid] | outgoing[nid]:
            reach2 |= incoming[x]
            reach2 |= outgoing[x]
        reach2.discard(nid)
        score = (
            KIND_WEIGHT.get(kind, 1.0) * 10
            + indeg * 4.5
            + outdeg * 5.0
            + len(reach2) * 1.2
            + neighbor_kinds.get('FrontendPage', 0) * 3.5
            + neighbor_kinds.get('APIRoute', 0) * 3.2
            + neighbor_kinds.get('Table', 0) * 4.0
        )
        severity = 'low'
        if score >= 42:
            severity = 'critical'
        elif score >= 28:
            severity = 'high'
        elif score >= 16:
            severity = 'medium'
        reasons = []
        if neighbor_kinds.get('FrontendPage'):
            reasons.append(f"touches {neighbor_kinds['FrontendPage']} frontend page(s)")
        if neighbor_kinds.get('APIRoute'):
            reasons.append(f"touches {neighbor_kinds['APIRoute']} API route(s)")
        if neighbor_kinds.get('Table'):
            reasons.append(f"connects to {neighbor_kinds['Table']} table node(s)")
        if reach2:
            reasons.append(f"two-hop blast radius {len(reach2)}")
        scored.append({
            'id': nid,
            'kind': kind,
            'name': n.get('label') or _node_name(nid),
            'title': n.get('title') or '',
            'score': round(score, 2),
            'severity': severity,
            'in_degree': indeg,
            'out_degree': outdeg,
            'two_hop_reach': len(reach2),
            'neighbor_kinds': dict(neighbor_kinds),
            'reasons': reasons[:5],
            'meta': n.get('meta') or {},
        })

    by_kind = defaultdict(list)
    for item in scored:
        by_kind[item['kind']].append(item)
    for kind in list(by_kind):
        by_kind[kind] = sorted(by_kind[kind], key=lambda x: (-x['score'], x['name']))

    hotspots = sorted(scored, key=lambda x: (-x['score'], x['kind'], x['name']))[:50]
    return {
        'ok': True,
        'summary': {
            'node_count': len(nodes),
            'edge_count': len(edges),
            'critical': sum(1 for x in scored if x['severity'] == 'critical'),
            'high': sum(1 for x in scored if x['severity'] == 'high'),
            'medium': sum(1 for x in scored if x['severity'] == 'medium'),
            'low': sum(1 for x in scored if x['severity'] == 'low'),
        },
        'hotspots': hotspots,
        'by_kind': dict(by_kind),
    }
