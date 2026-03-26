"""
DB Blueprint v2 — Connector Plugin System
==========================================
Every connector inherits BaseConnector and is auto-registered
by ConnectorRegistry on import.

To add a new DB:
  1. Create connectors/my_db.py
  2. class MyDBConnector(BaseConnector): ...
  3. Done — registry picks it up automatically.
"""

from __future__ import annotations

import abc
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConnectorField:
    key: str
    label: str
    type: str = "text"
    placeholder: str = ""
    default: Any = ""
    required: bool = True
    options: list = field(default_factory=list)
    width: str = "full"


class BaseConnector(abc.ABC):
    name: str = ""
    display_name: str = ""
    icon: str = "🔌"
    default_port: int = 0
    category: str = "relational"
    description: str = ""

    IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

    @classmethod
    def fields(cls) -> list[ConnectorField]:
        return [
            ConnectorField("host", "Host", placeholder="localhost", default="localhost", width="half"),
            ConnectorField("port", "Port", type="number", default=str(cls.default_port), width="third"),
            ConnectorField("schema", "Schema", placeholder="public", default="public", width="third"),
            ConnectorField("dbname", "Database", placeholder="mydb", width="full"),
            ConnectorField("user", "Username", placeholder="admin", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    @classmethod
    def meta(cls) -> dict:
        return {
            "name": cls.name,
            "display_name": cls.display_name,
            "icon": cls.icon,
            "default_port": cls.default_port,
            "category": cls.category,
            "description": cls.description,
            "fields": [f.__dict__ for f in cls.fields()],
        }

    @abc.abstractmethod
    def test_connection(self, **kwargs) -> dict:
        pass

    @abc.abstractmethod
    def extract_schema(self, **kwargs) -> dict:
        pass

    @abc.abstractmethod
    def get_table_list(self, **kwargs) -> list[str]:
        pass

    @abc.abstractmethod
    def get_table_detail(self, table: str, **kwargs) -> dict:
        pass

    @abc.abstractmethod
    def get_sample_rows(self, table: str, limit: int = 10, **kwargs) -> list[dict]:
        pass

    @staticmethod
    def _safe(v: Any, maxlen: int = 120) -> str:
        if v is None:
            return ""
        try:
            return str(v)[:maxlen]
        except Exception:
            return ""

    @staticmethod
    def _sanitize_rows(rows: list[dict]) -> list[dict]:
        return [{k: BaseConnector._safe(v) for k, v in r.items()} for r in rows]

    @staticmethod
    def _bounded_limit(limit: int, default: int = 10, maximum: int = 200) -> int:
        try:
            value = int(limit)
        except Exception:
            value = default
        return max(1, min(value, maximum))

    @classmethod
    def _validate_identifier(cls, value: str, label: str = "identifier") -> str:
        if not value or not isinstance(value, str) or not cls.IDENTIFIER_RE.match(value):
            raise ValueError(f"Invalid {label}. Use simple SQL identifiers only (letters, digits, underscore, dollar).")
        return value

    @classmethod
    def canonical_schema(cls, db_type: str, schema_name: str, tables: dict, explicit_fks: list, indexes: list) -> dict:
        return {
            "db_type": db_type,
            "schema_name": schema_name,
            "tables": tables,
            "explicit_fks": explicit_fks,
            "indexes": indexes,
        }


class ConnectorRegistry:
    _registry: dict[str, type[BaseConnector]] = {}

    @classmethod
    def register(cls, connector_cls: type[BaseConnector]) -> type[BaseConnector]:
        if not connector_cls.name:
            raise ValueError(f"{connector_cls.__name__} must define a 'name' attribute")
        cls._registry[connector_cls.name] = connector_cls
        return connector_cls

    @classmethod
    def get(cls, name: str) -> type[BaseConnector]:
        if name not in cls._registry:
            raise KeyError(f"No connector registered for '{name}'. Available: {list(cls._registry)}")
        return cls._registry[name]

    @classmethod
    def all(cls) -> dict[str, type[BaseConnector]]:
        return dict(cls._registry)

    @classmethod
    def all_meta(cls) -> list[dict]:
        return [c.meta() for c in cls._registry.values()]

    @classmethod
    def load_all(cls) -> None:
        import importlib
        import pkgutil
        import connectors as pkg

        for _, mod_name, _ in pkgutil.iter_modules(pkg.__path__):
            full = f"connectors.{mod_name}"
            if full not in ("connectors.base", "connectors.__init__"):
                importlib.import_module(full)
