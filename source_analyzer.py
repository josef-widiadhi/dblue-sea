from __future__ import annotations

import ast
import os
import re
import zipfile
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from settings import DATA_DIR

UPLOAD_DIR = DATA_DIR / 'source_uploads'
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ColumnDef:
    name: str
    type: str = ''
    nullable: bool | None = None
    primary_key: bool = False
    foreign_key: str = ''
    source_file: str = ''


@dataclass
class RelationDef:
    from_model: str
    from_column: str
    to_table: str
    to_column: str = 'id'
    confidence: str = 'high'
    source: str = 'orm_model'
    evidence: str = ''
    source_file: str = ''


@dataclass
class ModelDef:
    model_name: str
    table_name: str
    language: str
    framework: str
    columns: list[dict[str, Any]]
    relations: list[dict[str, Any]]
    source_file: str
    parser_mode: str = 'heuristic'


SCALAR_PRISMA_TYPES = {
    'String', 'Int', 'BigInt', 'Boolean', 'DateTime', 'Float', 'Decimal', 'Json', 'Bytes'
}


def _safe_name(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]+', '_', name).strip('._') or 'upload'


def save_upload(filename: str, data: bytes) -> dict[str, Any]:
    upload_id = _safe_name(Path(filename).stem) + '_' + re.sub(r'[^a-f0-9]', '', os.urandom(6).hex())
    zpath = UPLOAD_DIR / f'{upload_id}.zip'
    zpath.write_bytes(data)
    return {'upload_id': upload_id, 'filename': filename, 'path': str(zpath), 'size': len(data)}


def get_upload_path(upload_id: str) -> Path:
    p = UPLOAD_DIR / f'{_safe_name(upload_id)}.zip'
    if not p.exists():
        raise FileNotFoundError('Upload not found')
    return p


def _pluralize(name: str) -> str:
    n = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    if n.endswith('y') and not n.endswith(('ay', 'ey', 'iy', 'oy', 'uy')):
        return n[:-1] + 'ies'
    if n.endswith('s'):
        return n
    return n + 's'


def _asdict_list(items: list[Any]) -> list[dict[str, Any]]:
    out = []
    for item in items:
        out.append(asdict(item) if hasattr(item, '__dataclass_fields__') else item)
    return out


def _py_name(node: ast.AST | None) -> str:
    if node is None:
        return ''
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        left = _py_name(node.value)
        return f'{left}.{node.attr}' if left else node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Subscript):
        return _py_name(node.value)
    if isinstance(node, ast.Call):
        return _py_name(node.func)
    return ''


def _py_const(node: ast.AST | None):
    if isinstance(node, ast.Constant):
        return node.value
    return None


def _call_kw(call: ast.Call, key: str):
    for kw in call.keywords:
        if kw.arg == key:
            return _py_const(kw.value)
    return None


# ---- AST parsers (real AST upgrade) --------------------------------------

def _parse_sqla_python_ast(text: str, relpath: str) -> list[ModelDef]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    models: list[ModelDef] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        bases = [_py_name(b) for b in node.bases]
        if not any(b in {'Base', 'db.Model', 'DeclarativeBase', 'DeclarativeMeta'} or b.endswith('.Model') for b in bases):
            continue

        table = _pluralize(node.name)
        cols: list[ColumnDef] = []
        rels: list[RelationDef] = []
        for stmt in node.body:
            if not isinstance(stmt, ast.Assign):
                continue
            targets = [t.id for t in stmt.targets if isinstance(t, ast.Name)]
            if not targets:
                continue
            name = targets[0]
            if name == '__tablename__':
                sval = _py_const(stmt.value)
                if isinstance(sval, str) and sval:
                    table = sval
                continue
            if not isinstance(stmt.value, ast.Call):
                continue
            fname = _py_name(stmt.value.func)
            if fname.endswith('Column') or fname == 'Column':
                ctype = _py_name(stmt.value.args[0]) if stmt.value.args else ''
                pk = bool(_call_kw(stmt.value, 'primary_key'))
                nullable = _call_kw(stmt.value, 'nullable')
                fk_name = ''
                for arg in stmt.value.args[1:]:
                    if isinstance(arg, ast.Call) and (_py_name(arg.func).endswith('ForeignKey') or _py_name(arg.func) == 'ForeignKey'):
                        if arg.args:
                            fk_name = str(_py_const(arg.args[0]) or '')
                        break
                cols.append(ColumnDef(name=name, type=ctype, nullable=nullable if isinstance(nullable, bool) else None,
                                      primary_key=pk, foreign_key=fk_name, source_file=relpath))
                if fk_name:
                    to_table, to_col = (fk_name.split('.', 1) + ['id'])[:2] if '.' in fk_name else (fk_name, 'id')
                    rels.append(RelationDef(from_model=table, from_column=name, to_table=to_table,
                                            to_column=to_col, evidence='AST ForeignKey() declaration',
                                            source_file=relpath))
            elif fname.endswith('relationship') or fname == 'relationship':
                target_model = ''
                if stmt.value.args:
                    target_model = str(_py_const(stmt.value.args[0]) or _py_name(stmt.value.args[0]))
                if target_model:
                    rels.append(RelationDef(from_model=table, from_column=name,
                                            to_table=_pluralize(target_model.split('.')[-1]),
                                            evidence='AST relationship() declaration', source_file=relpath,
                                            confidence='medium'))
        if cols or rels:
            models.append(ModelDef(node.name, table, 'python', 'sqlalchemy/flask',
                                   _asdict_list(cols), _asdict_list(rels), relpath, parser_mode='ast'))
    return models


def _parse_django_python_ast(text: str, relpath: str) -> list[ModelDef]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    models: list[ModelDef] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        bases = [_py_name(b) for b in node.bases]
        if 'models.Model' not in bases and not any(b.endswith('.Model') for b in bases):
            continue

        table = _pluralize(node.name)
        cols: list[ColumnDef] = []
        rels: list[RelationDef] = []
        for stmt in node.body:
            if isinstance(stmt, ast.ClassDef) and stmt.name == 'Meta':
                for mstmt in stmt.body:
                    if isinstance(mstmt, ast.Assign):
                        for t in mstmt.targets:
                            if isinstance(t, ast.Name) and t.id == 'db_table':
                                sval = _py_const(mstmt.value)
                                if isinstance(sval, str) and sval:
                                    table = sval
            elif isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                targets = [t.id for t in stmt.targets if isinstance(t, ast.Name)]
                if not targets:
                    continue
                fname = _py_name(stmt.value.func)
                if not fname.startswith('models.'):
                    continue
                field_type = fname.split('.')[-1]
                field_name = targets[0]
                db_column = _call_kw(stmt.value, 'db_column') or field_name
                nullable = _call_kw(stmt.value, 'null')
                pk = bool(_call_kw(stmt.value, 'primary_key'))
                cols.append(ColumnDef(name=db_column, type=field_type,
                                      nullable=nullable if isinstance(nullable, bool) else None,
                                      primary_key=pk, source_file=relpath))
                if field_type in {'ForeignKey', 'OneToOneField'} and stmt.value.args:
                    target = str(_py_const(stmt.value.args[0]) or _py_name(stmt.value.args[0]))
                    rels.append(RelationDef(from_model=table, from_column=db_column,
                                            to_table=_pluralize(target.split('.')[-1]),
                                            evidence=f'AST {field_type} declaration', source_file=relpath))
        if cols or rels:
            models.append(ModelDef(node.name, table, 'python', 'django',
                                   _asdict_list(cols), _asdict_list(rels), relpath, parser_mode='ast'))
    return models


# ---- Heuristic / structured parsers --------------------------------------

def _parse_sqla_python(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    class_pat = re.compile(r'class\s+(\w+)\s*\(([^\)]*)\):([\s\S]*?)(?=^class\s+\w+\s*\(|\Z)', re.M)
    for cls, bases, body in class_pat.findall(text):
        if 'Base' not in bases and 'Declarative' not in bases and 'db.Model' not in bases:
            continue
        m_table = re.search(r'__tablename__\s*=\s*["\']([^"\']+)["\']', body)
        table = m_table.group(1) if m_table else _pluralize(cls)
        cols: list[ColumnDef] = []
        rels: list[RelationDef] = []
        for line in body.splitlines():
            line = line.strip()
            m_col = re.match(r'(\w+)\s*=\s*Column\((.+)\)', line)
            if m_col:
                cname, spec = m_col.groups()
                fk_match = re.search(r'ForeignKey\(["\']([^"\']+)["\']\)', spec)
                pk = 'primary_key=True' in spec
                nullable = None if 'nullable=' not in spec else ('nullable=False' not in spec)
                ctype = spec.split(',')[0].strip()
                cols.append(ColumnDef(name=cname, type=ctype, nullable=nullable, primary_key=pk,
                                      foreign_key=fk_match.group(1) if fk_match else '', source_file=relpath))
                if fk_match:
                    target = fk_match.group(1)
                    tt, tc = target.split('.', 1) if '.' in target else (target, 'id')
                    rels.append(RelationDef(from_model=table, from_column=cname, to_table=tt,
                                            to_column=tc, evidence='ForeignKey() declaration',
                                            source_file=relpath))
                continue
            m_rel = re.match(r'(\w+)\s*=\s*relationship\(["\'](\w+)["\']', line)
            if m_rel:
                attr, target_model = m_rel.groups()
                rels.append(RelationDef(from_model=table, from_column=attr,
                                        to_table=_pluralize(target_model), confidence='medium',
                                        evidence='relationship() declaration', source_file=relpath))
        if cols or rels or m_table:
            models.append(ModelDef(cls, table, 'python', 'sqlalchemy/flask',
                                   _asdict_list(cols), _asdict_list(rels), relpath, parser_mode='heuristic'))
    return models


def _parse_django_python(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    class_pat = re.compile(r'class\s+(\w+)\s*\(models\.Model\):([\s\S]*?)(?=^class\s+\w+\s*\(|\Z)', re.M)
    for cls, body in class_pat.findall(text):
        table = _pluralize(cls)
        meta = re.search(r'class\s+Meta:\s*([\s\S]*?)(?=^\S|\Z)', body, re.M)
        if meta:
            mdb = re.search(r'db_table\s*=\s*["\']([^"\']+)["\']', meta.group(1))
            if mdb:
                table = mdb.group(1)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        for line in body.splitlines():
            line = line.strip()
            m_field = re.match(r'(\w+)\s*=\s*models\.(\w+)\((.*)\)', line)
            if not m_field:
                continue
            fname, ftype, spec = m_field.groups()
            pk = 'primary_key=True' in spec
            nullable = None if ('null=' not in spec and 'blank=' not in spec) else ('null=False' not in spec)
            colname_match = re.search(r'db_column\s*=\s*["\']([^"\']+)["\']', spec)
            colname = colname_match.group(1) if colname_match else fname
            cols.append(asdict(ColumnDef(name=colname, type=ftype, nullable=nullable,
                                         primary_key=pk, source_file=relpath)))
            if ftype in {'ForeignKey', 'OneToOneField'}:
                target = re.search(r'models\.(ForeignKey|OneToOneField)\(([^,\)]+)', line)
                tgt = target.group(2).strip(" '\"") if target else 'unknown'
                rels.append(asdict(RelationDef(from_model=table, from_column=colname,
                                               to_table=_pluralize(tgt.split('.')[-1]),
                                               evidence=f'{ftype} declaration', source_file=relpath)))
        if cols or rels:
            models.append(ModelDef(cls, table, 'python', 'django', cols, rels, relpath, parser_mode='heuristic'))
    return models


def _parse_go_gorm(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    struct_pat = re.compile(r'type\s+(\w+)\s+struct\s*\{([\s\S]*?)\n\}', re.M)
    for sname, body in struct_pat.findall(text):
        table = _pluralize(sname)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        table_override = re.search(
            r'func\s*\(\s*\w+\s+\*?' + re.escape(sname) + r'\s*\)\s*TableName\(\)\s*string\s*\{\s*return\s+["\']([^"\']+)["\']',
            text,
        )
        if table_override:
            table = table_override.group(1)
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith('//'):
                continue
            m = re.match(r'(\w+)\s+(\*?\w+)(?:\s+`([^`]*)`)?', line)
            if not m:
                continue
            fname, ftype, tag = m.groups()
            tag = tag or ''
            gorm = re.search(r'gorm:"([^"]+)"', tag)
            col = re.search(r'column:([^;]+)', gorm.group(1)) if gorm else None
            pk = bool(re.search(r'primaryKey', gorm.group(1) if gorm else ''))
            fk = re.search(r'foreignKey:([^;]+);references:([^;]+)', gorm.group(1)) if gorm else None
            colname = col.group(1) if col else re.sub(r'(?<!^)(?=[A-Z])', '_', fname).lower()
            cols.append(asdict(ColumnDef(name=colname, type=ftype, primary_key=pk, source_file=relpath)))
            if fk:
                rels.append(asdict(RelationDef(from_model=table, from_column=fk.group(1),
                                               to_table=_pluralize(ftype.strip('*')),
                                               to_column=fk.group(2).lower(), evidence='gorm foreignKey tag',
                                               source_file=relpath)))
        if cols or rels:
            models.append(ModelDef(sname, table, 'go', 'gorm', cols, rels, relpath, parser_mode='heuristic'))
    return models


def _parse_php_laravel(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    class_pat = re.compile(r'class\s+(\w+)\s+extends\s+Model\b([\s\S]*?)\n\}', re.M)
    for cname, body in class_pat.findall(text):
        m_table = re.search(r'protected\s+\$table\s*=\s*["\']([^"\']+)["\']', body)
        table = m_table.group(1) if m_table else _pluralize(cname)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        fillable = re.search(r'\$fillable\s*=\s*\[([^\]]*)\]', body, re.S)
        if fillable:
            for raw in re.findall(r'["\']([^"\']+)["\']', fillable.group(1)):
                cols.append(asdict(ColumnDef(name=raw, type='mixed', source_file=relpath)))
        for meth in re.finditer(r'function\s+(\w+)\s*\([^\)]*\)\s*\{([\s\S]*?)\}', body):
            mname, mbody = meth.groups()
            rel = re.search(r'return\s+\$this->(belongsTo|hasOne|hasMany)\(([^\)]+)\)', mbody)
            if rel:
                kind = rel.group(1)
                target = rel.group(2).split(',')[0].strip(" '\\")
                rels.append(asdict(RelationDef(from_model=table, from_column=mname,
                                               to_table=_pluralize(target.split('\\\\')[-1]),
                                               confidence='medium', source='orm_model',
                                               evidence=f'Laravel {kind} relation', source_file=relpath)))
        if cols or rels or m_table:
            models.append(ModelDef(cname, table, 'php', 'laravel', cols, rels, relpath, parser_mode='heuristic'))
    return models


def _parse_typescript_typeorm(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    class_pat = re.compile(r'@Entity(?:\(([^)]*)\))?[\s\S]*?export\s+class\s+(\w+)\s*\{([\s\S]*?)\n\}', re.M)
    for entity_args, cname, body in class_pat.findall(text):
        table = ''
        m = re.search(r"['\"]([^'\"]+)['\"]", entity_args or '')
        if m:
            table = m.group(1)
        table = table or _pluralize(cname)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        pending: list[str] = []
        for raw in body.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith('@'):
                pending.append(line)
                continue
            m_field = re.match(r'(\w+)\s*:\s*([\w\[\]<>]+)', line)
            if not m_field:
                pending = []
                continue
            field, ftype = m_field.groups()
            decorators = []
            for deco_line in pending:
                m_deco = re.match(r'@(\w+)(?:\((.*)\))?', deco_line)
                if m_deco:
                    decorators.append(m_deco.groups())
            for deco, args in decorators:
                if deco in {'PrimaryGeneratedColumn', 'PrimaryColumn', 'Column'}:
                    cols.append(asdict(ColumnDef(name=field, type=ftype, primary_key=deco != 'Column', source_file=relpath)))
                elif deco in {'ManyToOne', 'OneToOne', 'OneToMany', 'ManyToMany'}:
                    target = ''
                    m2 = re.search(r'=>\s*(\w+)', args or '')
                    if m2:
                        target = m2.group(1)
                    target = target or ftype.replace('[]', '')
                    rels.append(asdict(RelationDef(from_model=table, from_column=field,
                                                   to_table=_pluralize(target), confidence='medium',
                                                   source='orm_model', evidence=f'TypeORM {deco} decorator',
                                                   source_file=relpath)))
            pending = []
        if cols or rels:
            models.append(ModelDef(cname, table, 'typescript', 'typeorm', cols, rels, relpath, parser_mode='heuristic'))
    return models


def _parse_prisma(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    model_pat = re.compile(r'model\s+(\w+)\s*\{([\s\S]*?)\n\}', re.M)
    for cname, body in model_pat.findall(text):
        table = _pluralize(cname)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        map_match = re.search(r"@@map\(\s*['\"]([^'\"]+)['\"]\s*\)", body)
        if map_match:
            table = map_match.group(1)
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith('//') or line.startswith('@@'):
                continue
            m = re.match(r'(\w+)\s+([\w\[\]?]+)\s*(.*)', line)
            if not m:
                continue
            fname, ftype, tail = m.groups()
            base_type = ftype.replace('[]', '').replace('?', '')
            is_pk = '@id' in tail
            cols.append(asdict(ColumnDef(name=fname, type=ftype, primary_key=is_pk,
                                         nullable='?' in ftype, source_file=relpath)))
            if base_type and base_type[0].isupper() and base_type not in SCALAR_PRISMA_TYPES:
                rels.append(asdict(RelationDef(from_model=table, from_column=fname,
                                               to_table=_pluralize(base_type), confidence='medium',
                                               source='orm_model', evidence='Prisma model relation',
                                               source_file=relpath)))
        if cols or rels:
            models.append(ModelDef(cname, table, 'prisma', 'prisma', cols, rels, relpath, parser_mode='structured'))
    return models


def _parse_java_hibernate(text: str, relpath: str) -> list[ModelDef]:
    models: list[ModelDef] = []
    class_pat = re.compile(r'@Entity[\s\S]*?class\s+(\w+)\s*\{([\s\S]*?)\n\}', re.M)
    for cname, body in class_pat.findall(text):
        table = _pluralize(cname)
        table_match = re.search(r"@Table\(name\s*=\s*['\"]([^'\"]+)['\"]", text)
        if table_match:
            table = table_match.group(1)
        cols: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        field_pat = re.compile(r'((?:@[A-Za-z0-9_()=\",\s]+\n\s*)*)(?:private|protected|public)\s+(\w+)\s+(\w+)\s*;', re.M)
        for annos, ftype, fname in field_pat.findall(body):
            annos = annos or ''
            col_name = fname
            col_match = re.search(r"@Column\(name\s*=\s*['\"]([^'\"]+)['\"]", annos)
            join_match = re.search(r"@JoinColumn\(name\s*=\s*['\"]([^'\"]+)['\"]", annos)
            if col_match:
                col_name = col_match.group(1)
            if join_match:
                col_name = join_match.group(1)
            cols.append(asdict(ColumnDef(name=col_name, type=ftype, primary_key='@Id' in annos, source_file=relpath)))
            rel_kind = re.search(r'@(ManyToOne|OneToOne|OneToMany|ManyToMany)', annos)
            if rel_kind:
                rels.append(asdict(RelationDef(from_model=table, from_column=col_name,
                                               to_table=_pluralize(ftype), confidence='medium',
                                               source='orm_model', evidence=f'Hibernate {rel_kind.group(1)} annotation',
                                               source_file=relpath)))
        if cols or rels:
            models.append(ModelDef(cname, table, 'java', 'hibernate/jpa', cols, rels, relpath, parser_mode='heuristic'))
    return models


def analyze_zip(path: Path) -> dict[str, Any]:
    models: list[ModelDef] = []
    scanned_files = 0
    parsed_by_mode: defaultdict[str, int] = defaultdict(int)
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            relpath = info.filename
            ext = Path(relpath).suffix.lower()
            if ext not in {'.py', '.php', '.go', '.ts', '.tsx', '.prisma', '.java'}:
                continue
            scanned_files += 1
            try:
                text = zf.read(info).decode('utf-8', errors='ignore')
            except Exception:
                continue

            file_models: list[ModelDef] = []
            if ext == '.py':
                file_models.extend(_parse_sqla_python_ast(text, relpath))
                file_models.extend(_parse_django_python_ast(text, relpath))
                if not file_models:
                    file_models.extend(_parse_sqla_python(text, relpath))
                    file_models.extend(_parse_django_python(text, relpath))
            elif ext == '.go':
                file_models.extend(_parse_go_gorm(text, relpath))
            elif ext == '.php':
                file_models.extend(_parse_php_laravel(text, relpath))
            elif ext in {'.ts', '.tsx'}:
                file_models.extend(_parse_typescript_typeorm(text, relpath))
            elif ext == '.prisma':
                file_models.extend(_parse_prisma(text, relpath))
            elif ext == '.java':
                file_models.extend(_parse_java_hibernate(text, relpath))

            for model in file_models:
                parsed_by_mode[model.parser_mode] += 1
            models.extend(file_models)

    uniq: dict[tuple[str, str], ModelDef] = {}
    for model in models:
        uniq[(model.table_name, model.source_file)] = model
    models = list(uniq.values())
    rel_count = sum(len(m.relations) for m in models)
    return {
        'ok': True,
        'scanned_files': scanned_files,
        'model_count': len(models),
        'relation_count': rel_count,
        'models': [asdict(m) for m in models],
        'summary': _summarize_models(models),
        'parser_modes': dict(parsed_by_mode),
        'support': {
            'ast': ['python: sqlalchemy/flask', 'python: django'],
            'structured': ['prisma'],
            'heuristic': ['php: laravel', 'go: gorm', 'typescript: typeorm', 'java: hibernate/jpa'],
        },
    }


def _summarize_models(models: list[ModelDef]) -> dict[str, Any]:
    by_lang = defaultdict(int)
    by_framework = defaultdict(int)
    by_mode = defaultdict(int)
    tables = []
    for model in models:
        by_lang[model.language] += 1
        by_framework[model.framework] += 1
        by_mode[model.parser_mode] += 1
        tables.append(model.table_name)
    return {
        'languages': dict(by_lang),
        'frameworks': dict(by_framework),
        'tables': sorted(set(tables)),
        'parser_modes': dict(by_mode),
    }


def diff_against_schema(source_result: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    db_tables = set((schema.get('tables') or {}).keys())
    source_tables = set(m['table_name'] for m in source_result.get('models', []))
    missing_in_db = sorted(source_tables - db_tables)
    missing_in_source = sorted(db_tables - source_tables)
    matched = sorted(source_tables & db_tables)
    column_mismatches = []
    for model in source_result.get('models', []):
        tname = model['table_name']
        if tname not in schema.get('tables', {}):
            continue
        db_cols = {c['name'] for c in schema['tables'][tname].get('columns', [])}
        src_cols = {c['name'] for c in model.get('columns', [])}
        missing_cols_db = sorted(src_cols - db_cols)
        missing_cols_source = sorted(db_cols - src_cols)
        if missing_cols_db or missing_cols_source:
            column_mismatches.append({
                'table': tname,
                'missing_in_db': missing_cols_db,
                'missing_in_source': missing_cols_source,
            })
    relations = []
    for model in source_result.get('models', []):
        for rel in model.get('relations', []):
            relations.append({
                'from_table': rel.get('from_model') or model['table_name'],
                'from_col': rel.get('from_column') or '',
                'to_table': rel.get('to_table') or '',
                'to_col': rel.get('to_column') or 'id',
                'rel_type': 'source_model',
                'confidence': rel.get('confidence') or 'high',
                'source': 'orm_model',
                'evidence': rel.get('evidence') or '',
                'source_file': rel.get('source_file') or model.get('source_file', ''),
                'parser_mode': model.get('parser_mode') or 'heuristic',
            })
    return {
        'ok': True,
        'matched_tables': matched,
        'missing_in_db': missing_in_db,
        'missing_in_source': missing_in_source,
        'column_mismatches': column_mismatches,
        'source_relations': relations,
    }
