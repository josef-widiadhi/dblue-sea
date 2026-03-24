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
from dataclasses import dataclass, field
from typing import Any


# ─── Field descriptor (drives the UI form) ───────────────────

@dataclass
class ConnectorField:
    key:         str
    label:       str
    type:        str   = "text"    # text | password | number | select
    placeholder: str   = ""
    default:     Any   = ""
    required:    bool  = True
    options:     list  = field(default_factory=list)   # for type=select
    width:       str   = "full"    # full | half | third


# ─── Base connector ───────────────────────────────────────────

class BaseConnector(abc.ABC):
    """
    Abstract base for all DB connectors.
    Subclass this, implement the abstract methods, and the
    ConnectorRegistry will pick it up automatically.
    """

    # ── Identity (override in subclass) ──────────────────────
    name:         str  = ""   # internal key, e.g. "postgresql"
    display_name: str  = ""   # shown in UI, e.g. "PostgreSQL"
    icon:         str  = "🔌" # emoji shown in UI
    default_port: int  = 0
    category:     str  = "relational"   # relational | nosql | warehouse | cache | search
    description:  str  = ""

    # ── Form fields (override to customise connection form) ───
    @classmethod
    def fields(cls) -> list[ConnectorField]:
        """Return the list of form fields for this connector."""
        return [
            ConnectorField("host",     "Host",     placeholder="localhost", default="localhost", width="half"),
            ConnectorField("port",     "Port",     type="number", default=str(cls.default_port), width="third"),
            ConnectorField("schema",   "Schema",   placeholder="public",    default="public",    width="third"),
            ConnectorField("dbname",   "Database", placeholder="mydb",      width="full"),
            ConnectorField("user",     "Username", placeholder="admin",     width="half"),
            ConnectorField("password", "Password", type="password",         width="half"),
        ]

    @classmethod
    def meta(cls) -> dict:
        """Serialisable metadata sent to the browser."""
        return {
            "name":         cls.name,
            "display_name": cls.display_name,
            "icon":         cls.icon,
            "default_port": cls.default_port,
            "category":     cls.category,
            "description":  cls.description,
            "fields":       [f.__dict__ for f in cls.fields()],
        }

    # ── Abstract interface ────────────────────────────────────

    @abc.abstractmethod
    def test_connection(self, **kwargs) -> dict:
        """
        Quick connectivity check.
        Returns: {"ok": bool, "table_count": int, "error": str|None}
        """

    @abc.abstractmethod
    def extract_schema(self, **kwargs) -> dict:
        """
        Full schema extraction: tables, columns, indexes, FKs, row counts.
        Returns the canonical schema dict.
        """

    @abc.abstractmethod
    def get_table_list(self, **kwargs) -> list[str]:
        """Return list of table/collection names (fast, no column detail)."""

    @abc.abstractmethod
    def get_table_detail(self, table: str, **kwargs) -> dict:
        """Return column+index detail for a single table (for explorer)."""

    @abc.abstractmethod
    def get_sample_rows(self, table: str, limit: int = 10, **kwargs) -> list[dict]:
        """Return sample rows from a table."""

    # ── Shared helpers ────────────────────────────────────────

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

    @classmethod
    def canonical_schema(
        cls,
        db_type: str,
        schema_name: str,
        tables: dict,
        explicit_fks: list,
        indexes: list,
    ) -> dict:
        """Build the standard schema dict returned by extract_schema."""
        return {
            "db_type":      db_type,
            "schema_name":  schema_name,
            "tables":       tables,
            "explicit_fks": explicit_fks,
            "indexes":      indexes,
        }


# ─── Registry ─────────────────────────────────────────────────

class ConnectorRegistry:
    _registry: dict[str, type[BaseConnector]] = {}

    @classmethod
    def register(cls, connector_cls: type[BaseConnector]) -> type[BaseConnector]:
        """Decorator — register a connector class."""
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
        """Serialisable list for the browser."""
        return [c.meta() for c in cls._registry.values()]

    @classmethod
    def load_all(cls) -> None:
        """Auto-import every .py file in the connectors/ folder."""
        import importlib
        import pkgutil
        import connectors as pkg
        for finder, mod_name, _ in pkgutil.iter_modules(pkg.__path__):
            full = f"connectors.{mod_name}"
            if full not in ("connectors.base", "connectors.__init__"):
                importlib.import_module(full)
