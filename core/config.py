"""
core/config.py
==============
All Market Pulse Pro v2 configuration is loaded exclusively from Railway
environment variables.  No hardcoded secrets or defaults anywhere.

Usage
-----
    from core.config import cfg, validate

    validate()          # call once at process startup
    print(cfg.REDIS_URL)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from typing import ClassVar


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _require(name: str) -> str:
    """Return the env-var value or raise a clear RuntimeError."""
    value = os.environ.get(name)
    if not value or not value.strip():
        raise RuntimeError(
            f"[config] Required environment variable '{name}' is missing or empty.\n"
            f"  → Set it in your Railway service's Variables panel before deploying."
        )
    return value.strip()


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """
    Immutable configuration object populated from environment variables.
    All attributes are strings; parse to int/bool at point of use.

    Required vars — startup will abort if any are absent:
        ANGELONE_API_KEY
        ANGELONE_CLIENT_ID
        ANGELONE_PASSWORD
        ANGELONE_TOTP_SECRET
        GROQ_API_KEY
        REDIS_URL

    Optional vars — have safe defaults:
        LOG_LEVEL          (default: "INFO")
        ENVIRONMENT        (default: "production")
        PORT               (default: "8000")
        CORS_ORIGINS       (default: "*")
    """

    # ------------------------------------------------------------------
    # AngelOne SmartAPI
    # ------------------------------------------------------------------
    ANGELONE_API_KEY: str
    ANGELONE_CLIENT_ID: str
    ANGELONE_PASSWORD: str
    ANGELONE_TOTP_SECRET: str

    # ------------------------------------------------------------------
    # AI / LLM
    # ------------------------------------------------------------------
    GROQ_API_KEY: str

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------
    REDIS_URL: str                 # e.g. redis://:password@host:6379/0

    # ------------------------------------------------------------------
    # Optional / operational
    # ------------------------------------------------------------------
    LOG_LEVEL: str = "INFO"
    ENVIRONMENT: str = "production"
    PORT: str = "8000"
    CORS_ORIGINS: str = "*"        # comma-separated list of allowed origins

    # ------------------------------------------------------------------
    # Derived / computed (not directly from env)
    # ------------------------------------------------------------------
    _REQUIRED: ClassVar[tuple[str, ...]] = (
        "ANGELONE_API_KEY",
        "ANGELONE_CLIENT_ID",
        "ANGELONE_PASSWORD",
        "ANGELONE_TOTP_SECRET",
        "GROQ_API_KEY",
        "REDIS_URL",
    )

    # Convenience: parsed CORS list
    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def port_int(self) -> int:
        try:
            return int(self.PORT)
        except ValueError:
            return 8000

    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT.lower() == "production"


# ---------------------------------------------------------------------------
# Module-level singleton — lazy-loaded on first import
# ---------------------------------------------------------------------------

def _load() -> Config:
    """Read every value from the environment (no validation here)."""
    return Config(
        # Required — we pass empty string if absent; validate() will catch it
        ANGELONE_API_KEY=os.environ.get("ANGELONE_API_KEY", ""),
        ANGELONE_CLIENT_ID=os.environ.get("ANGELONE_CLIENT_ID", ""),
        ANGELONE_PASSWORD=os.environ.get("ANGELONE_PASSWORD", ""),
        ANGELONE_TOTP_SECRET=os.environ.get("ANGELONE_TOTP_SECRET", ""),
        GROQ_API_KEY=os.environ.get("GROQ_API_KEY", ""),
        REDIS_URL=os.environ.get("REDIS_URL", ""),
        # Optional
        LOG_LEVEL=_optional("LOG_LEVEL", "INFO").upper(),
        ENVIRONMENT=_optional("ENVIRONMENT", "production").lower(),
        PORT=_optional("PORT", "8000"),
        CORS_ORIGINS=_optional("CORS_ORIGINS", "*"),
    )


cfg: Config = _load()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate() -> None:
    """
    Call exactly once at application startup (before any other module uses cfg).

    Checks every required variable is present and non-empty.  Raises
    ``RuntimeError`` with a human-readable list of all missing variables so
    the operator can fix them all in one deploy cycle.

    Example
    -------
        # main.py or app entrypoint
        from core.config import validate
        validate()
    """
    missing: list[str] = [
        name
        for name in Config._REQUIRED
        if not getattr(cfg, name, "").strip()
    ]

    if missing:
        lines = "\n".join(f"  • {name}" for name in missing)
        raise RuntimeError(
            f"[config] Startup aborted — {len(missing)} required environment "
            f"variable(s) are missing or empty:\n{lines}\n\n"
            f"  → Add them to your Railway service's Variables panel and redeploy."
        )

    # Sanity-check REDIS_URL scheme
    if not cfg.REDIS_URL.startswith(("redis://", "rediss://")):
        raise RuntimeError(
            f"[config] REDIS_URL must start with 'redis://' or 'rediss://', "
            f"got: '{cfg.REDIS_URL[:30]}...'"
        )

    _log_safe_summary()


def _log_safe_summary() -> None:
    """Print a redacted config summary so ops can confirm vars loaded."""
    def _mask(val: str, show: int = 4) -> str:
        return val[:show] + "***" if len(val) > show else "***"

    print(
        "[config] ✓ All required environment variables loaded.\n"
        f"  ANGELONE_CLIENT_ID : {_mask(cfg.ANGELONE_CLIENT_ID)}\n"
        f"  ANGELONE_API_KEY   : {_mask(cfg.ANGELONE_API_KEY)}\n"
        f"  GROQ_API_KEY       : {_mask(cfg.GROQ_API_KEY)}\n"
        f"  REDIS_URL          : {_mask(cfg.REDIS_URL, 14)}\n"
        f"  ENVIRONMENT        : {cfg.ENVIRONMENT}\n"
        f"  LOG_LEVEL          : {cfg.LOG_LEVEL}\n"
        f"  PORT               : {cfg.PORT}"
    )
