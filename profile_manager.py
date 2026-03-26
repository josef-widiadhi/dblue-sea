"""
DB Blueprint v2 — Profile Manager
Stores connection profiles in SQLite (data/profiles.db).
Credentials encrypted at rest with Fernet (AES-128-CBC).
"""

import base64
import hashlib
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet
from sqlalchemy import Boolean, Column, DateTime, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Session

from settings import DATA_DIR


# ─── Encryption key (derived from BLUEPRINT_SECRET only) ────────

def _derive_key() -> bytes:
    seed = os.environ.get("BLUEPRINT_SECRET", "db-blueprint-v2-local-key")
    # FIX SEC-3: removed HOSTNAME — unstable in Docker (changes per restart).
    # BLUEPRINT_SECRET alone is sufficient and portable across environments.
    # FIX SEC-4: warn loudly if default weak secret is in use.
    if seed == "db-blueprint-v2-local-key":
        import warnings
        warnings.warn(
            "BLUEPRINT_SECRET is not set — saved credentials use a weak default key. "
            "Set BLUEPRINT_SECRET in your .env file before storing real credentials.",
            stacklevel=2,
        )
    digest = hashlib.sha256(seed.encode()).digest()
    return base64.urlsafe_b64encode(digest)


_FERNET = Fernet(_derive_key())


def encrypt(plaintext: str) -> str:
    return _FERNET.encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    try:
        return _FERNET.decrypt(token.encode()).decode()
    except Exception as e:
        # FIX WARN-1: raise clearly — silent "" return causes confusing downstream errors
        raise ValueError(
            "Credential decryption failed. Check that BLUEPRINT_SECRET has not changed "
            f"since the profile was saved. Detail: {e}"
        ) from e


# ─── SQLAlchemy model ─────────────────────────────────────────

DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "profiles.db"

engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})


class Base(DeclarativeBase):
    pass


class Profile(Base):
    __tablename__ = "profiles"

    id = Column(String(36), primary_key=True)
    name = Column(String(120), nullable=False)
    db_type = Column(String(32), nullable=False)
    color = Column(String(16), default="#6c63ff")
    group_name = Column(String(80), default="")
    params_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_used = Column(DateTime, nullable=True)
    is_favourite = Column(Boolean, default=False)


Base.metadata.create_all(engine)


class ProfileManager:
    @staticmethod
    def _decrypt_params(p: Profile) -> tuple[dict, Optional[str]]:
        try:
            plaintext = decrypt(p.params_json)
        except ValueError as exc:
            return {}, str(exc)
        if not plaintext:
            return {}, "Unable to decrypt stored credentials. Check that BLUEPRINT_SECRET has not changed since this profile was saved."
        try:
            return json.loads(plaintext), None
        except json.JSONDecodeError:
            return {}, "Stored credentials are corrupted and could not be decoded."

    @classmethod
    def _row_to_dict(cls, p: Profile, include_creds: bool = False) -> dict:
        raw_params, decrypt_error = cls._decrypt_params(p)
        if include_creds:
            safe_params = raw_params
        else:
            safe_params = {k: ("••••••••" if k == "password" else v) for k, v in raw_params.items()}

        payload = {
            "id": p.id,
            "name": p.name,
            "db_type": p.db_type,
            "color": p.color,
            "group_name": p.group_name,
            "params": safe_params,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            "last_used": p.last_used.isoformat() if p.last_used else None,
            "is_favourite": p.is_favourite,
        }
        if decrypt_error:
            payload["decrypt_error"] = decrypt_error
        return payload

    @classmethod
    def list_profiles(cls) -> list[dict]:
        with Session(engine) as s:
            profiles = s.query(Profile).order_by(Profile.is_favourite.desc(), Profile.name).all()
            return [cls._row_to_dict(p) for p in profiles]

    @classmethod
    def get_profile(cls, profile_id: str, include_creds: bool = False) -> Optional[dict]:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if not p:
                return None
            return cls._row_to_dict(p, include_creds=include_creds)

    @classmethod
    def get_profile_params(cls, profile_id: str) -> Optional[dict]:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if not p:
                return None
            params, _ = cls._decrypt_params(p)
            return params or None

    @classmethod
    def create_profile(cls, name: str, db_type: str, params: dict, color: str = "#6c63ff", group_name: str = "") -> dict:
        pid = str(uuid.uuid4())
        with Session(engine) as s:
            p = Profile(
                id=pid,
                name=name,
                db_type=db_type,
                color=color,
                group_name=group_name,
                params_json=encrypt(json.dumps(params)),
            )
            s.add(p)
            s.commit()
            s.refresh(p)
            return cls._row_to_dict(p)

    @classmethod
    def update_profile(cls, profile_id: str, **fields) -> Optional[dict]:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if not p:
                return None
            if "name" in fields:
                p.name = fields["name"]
            if "color" in fields:
                p.color = fields["color"]
            if "group_name" in fields:
                p.group_name = fields["group_name"]
            if "params" in fields:
                p.params_json = encrypt(json.dumps(fields["params"]))
            if "is_favourite" in fields:
                p.is_favourite = fields["is_favourite"]
            p.updated_at = datetime.utcnow()
            s.commit()
            s.refresh(p)
            return cls._row_to_dict(p)

    @classmethod
    def delete_profile(cls, profile_id: str) -> bool:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if not p:
                return False
            s.delete(p)
            s.commit()
            return True

    @classmethod
    def mark_used(cls, profile_id: str) -> None:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if p:
                p.last_used = datetime.utcnow()
                s.commit()

    @classmethod
    def duplicate_profile(cls, profile_id: str) -> Optional[dict]:
        with Session(engine) as s:
            p = s.get(Profile, profile_id)
            if not p:
                return None
            params, _ = cls._decrypt_params(p)
            return cls.create_profile(
                name=f"{p.name} (copy)",
                db_type=p.db_type,
                params=params,
                color=p.color,
                group_name=p.group_name,
            )
