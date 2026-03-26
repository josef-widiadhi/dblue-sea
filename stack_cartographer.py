from __future__ import annotations

import ast
import hashlib
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import source_analyzer as sa
from graph import graph_store as gs


@dataclass
class RouteMap:
    method: str
    path: str
    handler_name: str
    handler_qualname: str
    source_file: str
    line_no: int
    models: list[str]
    tables: list[str]
    parser_mode: str = 'ast'
    framework: str = 'fastapi/flask'
    evidence: str = ''


@dataclass
class BackendFunctionMap:
    name: str
    qualname: str
    source_file: str
    line_no: int
    models: list[str]
    tables: list[str]
    called_functions: list[str]
    parser_mode: str = 'ast'


@dataclass
class FrontendNode:
    name: str
    kind: str
    source_file: str
    line_no: int
    framework: str
    parser_mode: str = 'heuristic'
    api_calls: list[str] | None = None
    page_routes: list[str] | None = None
    contains: list[str] | None = None


FETCH_PATTERNS = [
    r'''fetch\(\s*["'`]([^"'`]+)["'`]''',
    r'''axios\.(?:get|post|put|delete|patch)\(\s*["'`]([^"'`]+)["'`]''',
    r'''axios\(\s*\{[^}]*url\s*:\s*["'`]([^"'`]+)["'`]''',
    r'''\burl\s*:\s*["'`]([^"'`]+)["'`]''',
    r'''\baction\s*=\s*["'`]([^"'`]+)["'`]''',
    r'''\bhx-(?:get|post|put|delete|patch)\s*=\s*["'`]([^"'`]+)["'`]''',
    r'''apiClient\.(?:get|post|put|delete|patch)\(\s*["'`]([^"'`]+)["'`]''',
]


def _safe_hash(parts: list[str]) -> str:
    return hashlib.md5(':'.join(str(p) for p in parts).encode()).hexdigest()[:16]


def _py_name(node: ast.AST | None) -> str:
    if node is None:
        return ''
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        left = _py_name(node.value)
        return f'{left}.{node.attr}' if left else node.attr
    if isinstance(node, ast.Call):
        return _py_name(node.func)
    if isinstance(node, ast.Subscript):
        return _py_name(node.value)
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ''


def _const_str(node: ast.AST | None) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ''


def _normalize_api_path(path: str) -> str:
    path = (path or '').strip()
    if not path:
        return ''
    path = re.split(r'[?#]', path, 1)[0]
    path = path.replace('\\', '/')
    path = re.sub(r'/+', '/', path)
    if not path.startswith('/'):
        path = '/' + path
    path = re.sub(r'\$\{[^}]+\}', '', path)
    path = re.sub(r'\{[^}]+\}', '', path)
    path = re.sub(r':([A-Za-z_][A-Za-z0-9_]*)', '', path)
    if len(path) > 1 and path.endswith('/'):
        path = path[:-1]
    return path


class _FunctionInspector(ast.NodeVisitor):
    def __init__(self, known_models: set[str]):
        self.known_models = known_models
        self.names: set[str] = set()
        self.calls: set[str] = set()

    def visit_Name(self, node: ast.Name):
        self.names.add(node.id)

    def visit_Attribute(self, node: ast.Attribute):
        nm = _py_name(node)
        if nm:
            self.names.add(nm.split('.')[-1])
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        fn = _py_name(node.func)
        if fn:
            self.calls.add(fn)
        for arg in node.args:
            nm = _py_name(arg)
            if nm:
                self.names.add(nm.split('.')[-1])
        self.generic_visit(node)

    def result(self) -> tuple[list[str], list[str]]:
        models = sorted({n for n in self.names if n in self.known_models})
        calls = sorted(self.calls)
        return models, calls


class _PythonBackendAnalyzer(ast.NodeVisitor):
    def __init__(self, relpath: str, model_lookup: dict[str, str]):
        self.relpath = relpath
        self.model_lookup = model_lookup
        self.known_models = set(model_lookup)
        self.functions: list[BackendFunctionMap] = []
        self.routes: list[RouteMap] = []
        self.class_stack: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef):
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._handle_func(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._handle_func(node)
        self.generic_visit(node)

    def _decorator_route(self, deco: ast.AST) -> tuple[str, str, str]:
        if not isinstance(deco, ast.Call):
            return '', '', ''
        dname = _py_name(deco.func)
        method = ''
        framework = ''
        if any(dname.endswith(f'.{m}') or dname == m for m in ('get', 'post', 'put', 'delete', 'patch')):
            method = dname.split('.')[-1].upper()
            framework = 'fastapi/flask'
        elif dname.endswith('.route') or dname == 'route':
            framework = 'flask'
            path = _const_str(deco.args[0]) if deco.args else ''
            meths = []
            for kw in deco.keywords:
                if kw.arg == 'methods' and isinstance(kw.value, (ast.List, ast.Tuple)):
                    meths = [_const_str(el).upper() for el in kw.value.elts if _const_str(el)]
            method = ','.join(meths or ['GET'])
            return framework, method, path
        else:
            return '', '', ''
        path = _const_str(deco.args[0]) if deco.args else ''
        return framework, method, path

    def _handle_func(self, node: ast.AST):
        name = getattr(node, 'name', 'unknown')
        qual = '.'.join(self.class_stack + [name]) if self.class_stack else name
        ins = _FunctionInspector(self.known_models)
        ins.visit(node)
        models, calls = ins.result()
        tables = sorted({self.model_lookup[m] for m in models if m in self.model_lookup})
        line_no = getattr(node, 'lineno', 0)
        self.functions.append(BackendFunctionMap(name=name, qualname=qual, source_file=self.relpath, line_no=line_no,
                                models=models, tables=tables, called_functions=calls))
        for deco in getattr(node, 'decorator_list', []):
            framework, method, path = self._decorator_route(deco)
            if framework and path:
                self.routes.append(RouteMap(method=method, path=path, handler_name=name, handler_qualname=qual,
                                            source_file=self.relpath, line_no=line_no, models=models,
                                            tables=tables, framework=framework,
                                            evidence=f'{framework} decorator {method} {path}'))


def _guess_frontend_kind(relpath: str, text: str) -> tuple[str, str]:
    rel = relpath.lower()
    name = Path(relpath).stem
    if rel.endswith(('.html', '.jinja', '.jinja2')):
        return 'page', 'jinja/html'
    if 'components/' in rel or 'component' in name:
        return 'component', 'react/js'
    if any(seg in rel for seg in ('pages/', 'page.', 'screens/', 'screen.', 'views/', 'view.')):
        return 'page', 'react/js'
    if 'export default function' in text or 'return (' in text or rel.endswith(('.jsx', '.tsx')):
        return 'component', 'react/js'
    return 'component', 'js/ts'


def _extract_api_calls(text: str) -> list[str]:
    calls = set()
    for pat in FETCH_PATTERNS:
        for m in re.finditer(pat, text, flags=re.I | re.S):
            candidate = _normalize_api_path(m.group(1))
            if candidate.startswith('/api/'):
                calls.add(candidate)
    for m in re.finditer(r'''["'`](/api/[^"'`$+]*)(?:\$\{|["'`+])''', text, flags=re.I):
        calls.add(_normalize_api_path(m.group(1)))
    return sorted(calls)


def _extract_page_routes(text: str) -> list[str]:
    routes = set()
    for pat in [r'''href\s*=\s*["'`](/[^"'`?#]+)''', r'''to\s*=\s*["'`](/[^"'`?#]+)''']:
        for m in re.finditer(pat, text, flags=re.I):
            p = _normalize_api_path(m.group(1))
            if p and not p.startswith('/api/'):
                routes.add(p)
    return sorted(routes)


def _extract_component_refs(text: str) -> list[str]:
    refs = set()
    for m in re.finditer(r'''{%\s*include\s+["']([^"']+)["']\s*%}''', text):
        refs.add(Path(m.group(1)).name)
    for m in re.finditer(r'''<([A-Z][A-Za-z0-9_]*)\b''', text):
        refs.add(m.group(1))
    for m in re.finditer(r'''import\s+(?:\{?\s*([A-Z][A-Za-z0-9_]*)\s*\}?|([A-Z][A-Za-z0-9_]*))\s+from''', text):
        refs.add(next(g for g in m.groups() if g))
    return sorted(refs)


def _match_route(api_call: str, route_map: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    api_call = _normalize_api_path(api_call)
    if not api_call:
        return []
    exact = route_map.get(api_call)
    if exact:
        return exact
    matches = []
    for route_path, routes in route_map.items():
        rp = _normalize_api_path(route_path)
        if not rp:
            continue
        if api_call == rp or api_call.startswith(rp + '/') or rp.startswith(api_call + '/'):
            matches.extend(routes)
    return matches


def analyze_backend_api_db(path: Path) -> dict[str, Any]:
    source = sa.analyze_zip(path)
    models = source.get('models', [])
    model_lookup: dict[str, str] = {}
    model_nodes: list[dict[str, Any]] = []
    for m in models:
        model_lookup[m['model_name']] = m['table_name']
        model_nodes.append({
            'model_name': m['model_name'],
            'table_name': m['table_name'],
            'framework': m['framework'],
            'source_file': m['source_file'],
            'parser_mode': m.get('parser_mode', 'heuristic'),
        })

    functions: list[BackendFunctionMap] = []
    routes: list[RouteMap] = []
    frontend_nodes: list[FrontendNode] = []
    scanned_py = 0
    scanned_frontend = 0
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            rel = info.filename
            lower = rel.lower()
            try:
                text = zf.read(info).decode('utf-8', errors='ignore')
            except Exception:
                continue
            if lower.endswith('.py'):
                scanned_py += 1
                try:
                    tree = ast.parse(text)
                except Exception:
                    continue
                analyzer = _PythonBackendAnalyzer(rel, model_lookup)
                analyzer.visit(tree)
                functions.extend(analyzer.functions)
                routes.extend(analyzer.routes)
            elif lower.endswith(('.html', '.jinja', '.jinja2', '.js', '.jsx', '.ts', '.tsx')):
                scanned_frontend += 1
                kind, framework = _guess_frontend_kind(rel, text)
                frontend_nodes.append(FrontendNode(
                    name=Path(rel).stem,
                    kind=kind,
                    source_file=rel,
                    line_no=1,
                    framework=framework,
                    parser_mode='heuristic' if kind == 'component' else 'structured',
                    api_calls=_extract_api_calls(text),
                    page_routes=_extract_page_routes(text),
                    contains=_extract_component_refs(text),
                ))

    uniq_routes: dict[tuple[str, str, str, str], RouteMap] = {}
    for r in routes:
        uniq_routes[(r.method, r.path, r.handler_qualname, r.source_file)] = r
    route_nodes = [asdict(r) for r in uniq_routes.values()]
    function_nodes = [asdict(f) for f in functions]
    frontend_dicts = [asdict(n) for n in frontend_nodes]

    table_usage = defaultdict(lambda: {'route_count': 0, 'routes': [], 'handlers': []})
    for r in route_nodes:
        for t in r['tables']:
            entry = table_usage[t]
            entry['route_count'] += 1
            entry['routes'].append(f"{r['method']} {r['path']}")
            entry['handlers'].append(r['handler_qualname'])

    route_map = defaultdict(list)
    for r in route_nodes:
        route_map[_normalize_api_path(r['path'])].append(r)

    frontend_usage = defaultdict(lambda: {'apis': [], 'tables': [], 'handlers': []})
    page_table_usage = defaultdict(set)
    component_table_usage = defaultdict(set)
    edges = []

    for m in model_nodes:
        edges.append({'from_type': 'ORMModel', 'from': m['model_name'], 'to_type': 'Table', 'to': m['table_name'], 'rel': 'MAPS_TO', 'source_file': m['source_file']})
    for f in function_nodes:
        for m in f['models']:
            edges.append({'from_type': 'BackendFunction', 'from': f['qualname'], 'to_type': 'ORMModel', 'to': m, 'rel': 'USES_MODEL', 'source_file': f['source_file']})
        for t in f['tables']:
            edges.append({'from_type': 'BackendFunction', 'from': f['qualname'], 'to_type': 'Table', 'to': t, 'rel': 'TOUCHES_TABLE', 'source_file': f['source_file']})
        for callee in f['called_functions'][:25]:
            edges.append({'from_type': 'BackendFunction', 'from': f['qualname'], 'to_type': 'BackendFunction', 'to': callee, 'rel': 'CALLS_FUNCTION', 'source_file': f['source_file']})
    for r in route_nodes:
        edges.append({'from_type': 'APIRoute', 'from': f"{r['method']} {r['path']}", 'to_type': 'BackendFunction', 'to': r['handler_qualname'], 'rel': 'HANDLED_BY', 'source_file': r['source_file']})
        for t in r['tables']:
            edges.append({'from_type': 'APIRoute', 'from': f"{r['method']} {r['path']}", 'to_type': 'Table', 'to': t, 'rel': 'USES_TABLE', 'source_file': r['source_file']})

    component_names = {n['name']: n for n in frontend_dicts if n['kind'] == 'component'}
    for n in frontend_dicts:
        node_type = 'FrontendPage' if n['kind'] == 'page' else 'FrontendComponent'
        for api in n.get('api_calls') or []:
            matched = _match_route(api, route_map)
            frontend_usage[n['name']]['apis'].append(api)
            for r in matched:
                route_name = f"{r['method']} {r['path']}"
                edges.append({'from_type': node_type, 'from': n['name'], 'to_type': 'APIRoute', 'to': route_name, 'rel': 'CALLS_API', 'source_file': n['source_file']})
                frontend_usage[n['name']]['apis'].append(route_name)
                frontend_usage[n['name']]['handlers'].append(r['handler_qualname'])
                for t in r.get('tables') or []:
                    frontend_usage[n['name']]['tables'].append(t)
                    if n['kind'] == 'page':
                        page_table_usage[n['name']].add(t)
                    else:
                        component_table_usage[n['name']].add(t)
                    edges.append({'from_type': node_type, 'from': n['name'], 'to_type': 'Table', 'to': t, 'rel': 'USES_TABLE', 'source_file': n['source_file']})
        if n['kind'] == 'page':
            for ref in n.get('contains') or []:
                target = ref if ref in component_names else next((k for k in component_names if k.lower() == Path(ref).stem.lower()), None)
                if target:
                    edges.append({'from_type': 'FrontendPage', 'from': n['name'], 'to_type': 'FrontendComponent', 'to': target, 'rel': 'CONTAINS', 'source_file': n['source_file']})

    for n in frontend_dicts:
        if n['kind'] != 'page':
            continue
        for ref in n.get('contains') or []:
            target = ref if ref in component_table_usage else next((k for k in component_table_usage if k.lower() == Path(ref).stem.lower()), None)
            if target:
                page_table_usage[n['name']].update(component_table_usage[target])
        for t in sorted(page_table_usage[n['name']]):
            edges.append({'from_type': 'FrontendPage', 'from': n['name'], 'to_type': 'Table', 'to': t, 'rel': 'USES_TABLE', 'source_file': n['source_file']})

    summary = {
        'python_files_scanned': scanned_py,
        'frontend_files_scanned': scanned_frontend,
        'orm_models': len(model_nodes),
        'backend_functions': len(function_nodes),
        'api_routes': len(route_nodes),
        'frontend_pages': sum(1 for n in frontend_dicts if n['kind'] == 'page'),
        'frontend_components': sum(1 for n in frontend_dicts if n['kind'] == 'component'),
        'frontend_api_links': sum(1 for e in edges if e['rel'] == 'CALLS_API' and e['from_type'].startswith('Frontend')),
        'page_table_links': sum(len(v) for v in page_table_usage.values()),
        'tables_touched_by_routes': len(table_usage),
        'top_tables': sorted(
            [{'table': k, 'route_count': v['route_count'], 'routes': sorted(set(v['routes']))[:8], 'handlers': sorted(set(v['handlers']))[:8]} for k, v in table_usage.items()],
            key=lambda x: (-x['route_count'], x['table'])
        )[:20],
        'top_frontend_pages': sorted(
            [
                {
                    'page': n['name'],
                    'file': n['source_file'],
                    'api_count': len(set(frontend_usage[n['name']]['apis'])),
                    'tables': sorted(page_table_usage.get(n['name'], set()))[:12],
                    'handlers': sorted(set(frontend_usage[n['name']]['handlers']))[:8],
                }
                for n in frontend_dicts if n['kind'] == 'page'
            ],
            key=lambda x: (-x['api_count'], x['page'])
        )[:20],
    }
    visual = build_visual_graph({
        'models': model_nodes,
        'functions': function_nodes,
        'routes': route_nodes,
        'frontend': frontend_dicts,
        'edges': edges,
        'table_usage': [{**{'table': t}, **v, 'routes': sorted(set(v['routes'])), 'handlers': sorted(set(v['handlers']))} for t, v in sorted(table_usage.items())],
    })
    return {
        'ok': True,
        'summary': summary,
        'visual': visual,
        'models': model_nodes,
        'functions': function_nodes,
        'routes': route_nodes,
        'frontend': frontend_dicts,
        'edges': edges,
        'table_usage': [{**{'table': t}, **v, 'routes': sorted(set(v['routes'])), 'handlers': sorted(set(v['handlers']))} for t, v in sorted(table_usage.items())],
        'page_usage': [
            {
                'page': n['name'],
                'file': n['source_file'],
                'framework': n['framework'],
                'api_calls': sorted(set(frontend_usage[n['name']]['apis'] or n.get('api_calls') or [])),
                'tables': sorted(page_table_usage.get(n['name'], set()) or set(frontend_usage[n['name']]['tables'])),
                'handlers': sorted(set(frontend_usage[n['name']]['handlers'])),
                'contains': n.get('contains') or [],
            }
            for n in frontend_dicts if n['kind'] == 'page'
        ],
        'component_usage': [
            {
                'component': n['name'],
                'file': n['source_file'],
                'framework': n['framework'],
                'api_calls': sorted(set(frontend_usage[n['name']]['apis'] or n.get('api_calls') or [])),
                'tables': sorted(component_table_usage.get(n['name'], set()) or set(frontend_usage[n['name']]['tables'])),
                'handlers': sorted(set(frontend_usage[n['name']]['handlers'])),
            }
            for n in frontend_dicts if n['kind'] == 'component'
        ],
        'support': {
            'implemented': ['python fastapi', 'python flask', 'python sqlalchemy', 'python django models', 'jinja/html api call mapping', 'js fetch', 'axios', 'typescript client call mapping'],
            'next': ['service/repository chain deepening', 'typescript AST route/client extraction', 'react component tree AST'],
        },
    }




def build_visual_graph(cart: dict[str, Any]) -> dict[str, Any]:
    type_style = {
        'FrontendPage': {'group':'frontend_page','color':'#22d3a0','shape':'box'},
        'FrontendComponent': {'group':'frontend_component','color':'#34d399','shape':'ellipse'},
        'APIRoute': {'group':'api_route','color':'#6c63ff','shape':'box'},
        'BackendFunction': {'group':'backend_fn','color':'#a78bfa','shape':'diamond'},
        'ORMModel': {'group':'orm_model','color':'#f59e0b','shape':'ellipse'},
        'Table': {'group':'table','color':'#f87171','shape':'database'},
    }
    nodes = {}
    edges = []

    def add_node(node_type: str, node_id: str, label: str, title: str = '', meta: dict[str, Any] | None = None):
        if not node_id:
            return
        style = type_style.get(node_type, {'group':'other','color':'#8888a8','shape':'dot'})
        node_key = f'{node_type}:{node_id}'
        if node_key not in nodes:
            nodes[node_key] = {
                'id': node_key,
                'label': label,
                'title': title or label,
                'kind': node_type,
                'group': style['group'],
                'shape': style['shape'],
                'color': style['color'],
                'meta': meta or {},
            }

    for n in cart.get('frontend', []):
        add_node('FrontendPage' if n.get('kind') == 'page' else 'FrontendComponent', n.get('name',''), n.get('name',''), n.get('source_file',''), {'source_file':n.get('source_file',''), 'framework':n.get('framework','')})
    for n in cart.get('routes', []):
        route_name = f"{n.get('method','')} {n.get('path','')}".strip()
        add_node('APIRoute', route_name, route_name, n.get('source_file',''), {'source_file':n.get('source_file',''), 'handler':n.get('handler_qualname','')})
    for n in cart.get('functions', []):
        add_node('BackendFunction', n.get('qualname',''), n.get('qualname',''), n.get('source_file',''), {'source_file':n.get('source_file',''), 'tables':n.get('tables',[])})
    for n in cart.get('models', []):
        add_node('ORMModel', n.get('model_name',''), n.get('model_name',''), n.get('source_file',''), {'table_name':n.get('table_name',''), 'source_file':n.get('source_file','')})
        add_node('Table', n.get('table_name',''), n.get('table_name',''), n.get('table_name',''), {})
    for n in cart.get('table_usage', []):
        add_node('Table', n.get('table',''), n.get('table',''), n.get('table',''), {'route_count':n.get('route_count',0)})

    seen_edges = set()
    for e in cart.get('edges', []):
        source_key = f"{e.get('from_type')}:{e.get('from')}"
        target_key = f"{e.get('to_type')}:{e.get('to')}"
        if source_key not in nodes:
            add_node(e.get('from_type','Other'), e.get('from',''), e.get('from',''), e.get('source_file',''), {})
        if target_key not in nodes:
            add_node(e.get('to_type','Other'), e.get('to',''), e.get('to',''), e.get('source_file',''), {})
        ek = (source_key, target_key, e.get('rel',''))
        if ek in seen_edges:
            continue
        seen_edges.add(ek)
        edges.append({
            'id': f'e{len(edges)+1}',
            'from': source_key,
            'to': target_key,
            'label': e.get('rel',''),
            'title': f"{e.get('rel','')}\n{e.get('source_file','')}",
            'kind': e.get('rel',''),
            'source_file': e.get('source_file',''),
        })

    summary = {
        'node_count': len(nodes),
        'edge_count': len(edges),
        'by_kind': {},
    }
    for n in nodes.values():
        summary['by_kind'][n['kind']] = summary['by_kind'].get(n['kind'], 0) + 1
    return {'nodes': list(nodes.values()), 'edges': edges, 'summary': summary}

def ingest_stack_map(profile_id: str, profile_name: str, cart: dict[str, Any]) -> dict[str, Any]:
    if not gs.is_available():
        return {'ok': False, 'ingested': False, 'reason': 'Neo4j unavailable'}
    driver = gs._get_driver()
    with driver.session() as session:
        gs._ensure_constraints(session)
        for stmt in [
            'CREATE CONSTRAINT api_route_id IF NOT EXISTS FOR (n:APIRoute) REQUIRE n.id IS UNIQUE',
            'CREATE CONSTRAINT backend_fn_id IF NOT EXISTS FOR (n:BackendFunction) REQUIRE n.id IS UNIQUE',
            'CREATE CONSTRAINT orm_model_id IF NOT EXISTS FOR (n:ORMModel) REQUIRE n.id IS UNIQUE',
            'CREATE CONSTRAINT frontend_page_id IF NOT EXISTS FOR (n:FrontendPage) REQUIRE n.id IS UNIQUE',
            'CREATE CONSTRAINT frontend_component_id IF NOT EXISTS FOR (n:FrontendComponent) REQUIRE n.id IS UNIQUE',
        ]:
            session.run(stmt)

        model_source_lookup = {}
        for m in cart.get('models', []):
            mid = _safe_hash([profile_id, 'orm', m['model_name'], m['source_file']])
            model_source_lookup[m['model_name']] = m['source_file']
            tid = gs._node_id([gs._node_id([profile_id, 'default']), m['table_name']])
            session.run(
                '''
                MERGE (m:ORMModel {id:$id})
                SET m.name=$name, m.table_name=$table_name, m.profile_id=$profile_id,
                    m.profile_name=$profile_name, m.framework=$framework, m.source_file=$source_file,
                    m.parser_mode=$parser_mode
                ''',
                id=mid, name=m['model_name'], table_name=m['table_name'], profile_id=profile_id,
                profile_name=profile_name, framework=m.get('framework',''), source_file=m['source_file'], parser_mode=m.get('parser_mode','heuristic')
            )
            session.run(
                '''
                MERGE (t:Table {id:$id})
                ON CREATE SET t.name=$name, t.profile_id=$profile_id, t.profile_name=$profile_name, t.schema_name='default'
                ''', id=tid, name=m['table_name'], profile_id=profile_id, profile_name=profile_name
            )
            session.run('MATCH (m:ORMModel {id:$mid}),(t:Table {id:$tid}) MERGE (m)-[:MAPS_TO]->(t)', mid=mid, tid=tid)

        fn_ids = {}
        for f in cart.get('functions', []):
            fid = _safe_hash([profile_id, 'fn', f['qualname'], f['source_file']])
            fn_ids[f['qualname']] = fid
            session.run(
                '''
                MERGE (f:BackendFunction {id:$id})
                SET f.name=$name, f.qualname=$qualname, f.profile_id=$profile_id, f.profile_name=$profile_name,
                    f.source_file=$source_file, f.line_no=$line_no, f.parser_mode=$parser_mode
                ''',
                id=fid, name=f['name'], qualname=f['qualname'], profile_id=profile_id, profile_name=profile_name,
                source_file=f['source_file'], line_no=f.get('line_no',0), parser_mode=f.get('parser_mode','ast')
            )
            for model_name in f.get('models', []):
                mid = _safe_hash([profile_id, 'orm', model_name, model_source_lookup.get(model_name, '')])
                session.run('MATCH (f:BackendFunction {id:$fid}),(m:ORMModel {id:$mid}) MERGE (f)-[:USES_MODEL]->(m)', fid=fid, mid=mid)
            for table_name in f.get('tables', []):
                tid = gs._node_id([gs._node_id([profile_id, 'default']), table_name])
                session.run('MATCH (f:BackendFunction {id:$fid}),(t:Table {id:$tid}) MERGE (f)-[:TOUCHES_TABLE]->(t)', fid=fid, tid=tid)

        route_ids = {}
        for r in cart.get('routes', []):
            rid = _safe_hash([profile_id, 'route', r['method'], r['path']])
            route_ids[f"{r['method']} {r['path']}"] = rid
            session.run(
                '''
                MERGE (r:APIRoute {id:$id})
                SET r.method=$method, r.path=$path, r.profile_id=$profile_id, r.profile_name=$profile_name,
                    r.source_file=$source_file, r.line_no=$line_no, r.framework=$framework, r.parser_mode=$parser_mode
                ''',
                id=rid, method=r['method'], path=r['path'], profile_id=profile_id, profile_name=profile_name,
                source_file=r['source_file'], line_no=r.get('line_no',0), framework=r.get('framework',''), parser_mode=r.get('parser_mode','ast')
            )
            fid = fn_ids.get(r['handler_qualname'])
            if fid:
                session.run('MATCH (r:APIRoute {id:$rid}),(f:BackendFunction {id:$fid}) MERGE (r)-[:HANDLED_BY]->(f)', rid=rid, fid=fid)
            for table_name in r.get('tables', []):
                tid = gs._node_id([gs._node_id([profile_id, 'default']), table_name])
                session.run('MATCH (r:APIRoute {id:$rid}),(t:Table {id:$tid}) MERGE (r)-[:USES_TABLE]->(t)', rid=rid, tid=tid)

        page_ids = {}
        component_ids = {}
        for n in cart.get('frontend', []):
            label = 'FrontendPage' if n.get('kind') == 'page' else 'FrontendComponent'
            nid = _safe_hash([profile_id, label.lower(), n['name'], n['source_file']])
            if label == 'FrontendPage':
                page_ids[n['name']] = nid
            else:
                component_ids[n['name']] = nid
            session.run(
                f'''
                MERGE (n:{label} {{id:$id}})
                SET n.name=$name, n.profile_id=$profile_id, n.profile_name=$profile_name,
                    n.source_file=$source_file, n.line_no=$line_no, n.framework=$framework, n.parser_mode=$parser_mode
                ''',
                id=nid, name=n['name'], profile_id=profile_id, profile_name=profile_name,
                source_file=n['source_file'], line_no=n.get('line_no',1), framework=n.get('framework',''), parser_mode=n.get('parser_mode','heuristic')
            )

        for e in cart.get('edges', []):
            from_type = e.get('from_type')
            to_type = e.get('to_type')
            if from_type == 'FrontendPage':
                fid = page_ids.get(e['from'])
            elif from_type == 'FrontendComponent':
                fid = component_ids.get(e['from'])
            elif from_type == 'APIRoute':
                fid = route_ids.get(e['from'])
            elif from_type == 'BackendFunction':
                fid = fn_ids.get(e['from'])
            elif from_type == 'ORMModel':
                fid = _safe_hash([profile_id, 'orm', e['from'], model_source_lookup.get(e['from'], '')])
            else:
                fid = None
            if to_type == 'FrontendPage':
                tid = page_ids.get(e['to'])
            elif to_type == 'FrontendComponent':
                tid = component_ids.get(e['to'])
            elif to_type == 'APIRoute':
                tid = route_ids.get(e['to'])
            elif to_type == 'BackendFunction':
                tid = fn_ids.get(e['to'])
            elif to_type == 'ORMModel':
                tid = _safe_hash([profile_id, 'orm', e['to'], model_source_lookup.get(e['to'], '')])
            elif to_type == 'Table':
                tid = gs._node_id([gs._node_id([profile_id, 'default']), e['to']])
            else:
                tid = None
            if not fid or not tid:
                continue
            session.run(f"MATCH (a {{id:$aid}}),(b {{id:$bid}}) MERGE (a)-[:{e['rel']}]->(b)", aid=fid, bid=tid)

    return {
        'ok': True,
        'ingested': True,
        'routes': len(cart.get('routes', [])),
        'functions': len(cart.get('functions', [])),
        'models': len(cart.get('models', [])),
        'frontend': len(cart.get('frontend', [])),
    }
