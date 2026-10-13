"""Run configuration: smoke-vs-full, periodo resolution with latest fallback.

Smoke (validation) pins a small slice: datasets=[cnaes], ufs=[MA].
Full runs everything. Periodo resolves explicitly, else latest available
``_SUCCESS`` marker (fixes the bug where empty periodo meant "current month").
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

DEFAULT_UFS_BRASIL: list[str] = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]

DATASET_PREFIX: dict[str, str] = {
    "estabelecimentos": "Estabelecimentos",
    "empresas": "Empresas",
    "socios": "Socios",
    "cnaes": "Cnaes",
}

SMOKE_DATASETS: list[str] = ["cnaes"]
SMOKE_UFS: list[str] = ["MA"]
FULL_DATASETS: list[str] = ["cnaes", "empresas", "estabelecimentos", "socios"]


class MarkerStore(Protocol):
    """Minimal interface satisfied by MinIO client wrappers (testable)."""

    def has_success(self, bucket: str, prefix: str, date_suffix: str) -> bool: ...


def validate_periodo(value: str) -> str:
    """Validate ``YYYY-MM`` and return normalized value."""
    import re

    normalized = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}", normalized):
        raise ValueError(f"periodo inválido: {normalized}. Use YYYY-MM")
    return normalized


def get_last_day_of_month(year_month: str) -> str:
    """Return last day of month as ``YYYYMMDD``."""
    year, month = map(int, validate_periodo(year_month).split("-"))
    return f"{year:04d}{month:02d}{monthrange(year, month)[1]:02d}"


def _airflow_var(key: str, env_name: str = "", default: str = "") -> str:
    """Read an Airflow Variable with env fallback (test-friendly).

    Resolution order (Airflow-native): file backend > AIRFLOW_VAR_* > DB.
    Without airflow installed (unit tests) falls back to ``os.environ``.
    """
    value = ""
    try:
        try:
            # Airflow 3 SDK: get(key, default=...).
            from airflow.sdk import Variable

            fetched = Variable.get(key, default="")
        except ImportError:
            # Airflow 2 / SDK antigo: get(key, default_var=...).
            from airflow.models import Variable as ClassicVariable

            fetched = ClassicVariable.get(key, default_var="")
        if fetched:
            value = str(fetched).strip()
    except Exception:
        value = ""
    if value:
        return value
    if env_name:
        import os

        return os.environ.get(env_name, default).strip()
    return default


def resolve_ufs(params: dict[str, Any] | None = None) -> list[str]:
    """Resolve target UFs: params estado/ufs > Variable/env > all (smoke callers pin explicitly)."""
    import re

    if params:
        estado = str(params.get("estado", "") or params.get("ufs", "")).strip().upper()
        if estado:
            parts = [p.strip() for p in re.split(r"[,;|]", estado) if p.strip()]
            invalid = sorted({u for u in parts if u not in DEFAULT_UFS_BRASIL})
            if invalid:
                raise ValueError(f"UFs inválidas: {invalid}")
            return parts
    env_estado = _airflow_var("processing_estado", "PROCESSING_ESTADO").upper()
    if env_estado:
        if env_estado not in DEFAULT_UFS_BRASIL:
            raise ValueError(f"PROCESSING_ESTADO inválido: {env_estado}")
        return [env_estado]
    return list(DEFAULT_UFS_BRASIL)


def _parse_env() -> str:
    return _airflow_var("processing_periodo", "PROCESSING_PERIODO")


def duckdb_settings() -> tuple[str, int]:
    """Tuning DuckDB via Variables (fallback env/.env, defaults 2GB/2).

    Variables: ``duckdb_memory_limit`` (ex. "2GB", "2.0", "2") e
    ``duckdb_threads`` (ex. "2"). Envs: ``DUCKDB_MEMORY_LIMIT_GB`` /
    ``DUCKDB_THREADS``. Sempre retorna par válido (fail-safe).
    """
    raw_mem = _airflow_var("duckdb_memory_limit", "DUCKDB_MEMORY_LIMIT_GB", "2.0").strip().upper()
    memory = "2GB"
    if raw_mem:
        try:
            if raw_mem.endswith("GB"):
                memory = f"{float(raw_mem[:-2]):g}GB"
            elif raw_mem.endswith("MB"):
                memory = f"{float(raw_mem[:-2]):g}MB"
            else:
                memory = f"{float(raw_mem):g}GB"
        except ValueError:
            memory = "2GB"
    raw_thr = _airflow_var("duckdb_threads", "DUCKDB_THREADS", "2").strip()
    try:
        threads = max(1, int(float(raw_thr)))
    except ValueError:
        threads = 2
    return memory, threads


def resolve_periodo_or_latest(
    store: MarkerStore,
    bucket: str,
    bronze_prefix: str,
    params: dict[str, Any] | None = None,
) -> str:
    """Resolve ``YYYY-MM``: explicit param/env, else latest month with ``_SUCCESS``.

    Falls back up to 6 months back from current month.
    """
    from dateutil.relativedelta import relativedelta

    explicit = ""
    if params and str(params.get("periodo", "")).strip():
        explicit = validate_periodo(str(params["periodo"]))
    elif _parse_env():
        explicit = validate_periodo(_parse_env())
    if explicit:
        suffix = get_last_day_of_month(explicit)
        if not store.has_success(bucket, bronze_prefix, suffix):
            raise ValueError(f"Bronze sem _SUCCESS para {explicit} ({bronze_prefix})")
        return explicit

    now = datetime.now()
    for back in range(6):
        candidate: str = (now - relativedelta(months=back)).strftime("%Y-%m")
        if store.has_success(bucket, bronze_prefix, get_last_day_of_month(candidate)):
            return candidate
    raise ValueError(f"Bronze sem _SUCCESS nos últimos 6 meses ({bronze_prefix})")


@dataclass(frozen=True)
class RunParams:
    """Validated run slice shared by all DAGs."""

    modo: str = "smoke"
    periodo: str = ""
    ufs: tuple[str, ...] = field(default_factory=lambda: tuple(SMOKE_UFS))
    datasets: tuple[str, ...] = field(default_factory=lambda: tuple(SMOKE_DATASETS))

    @classmethod
    def from_dag_params(cls, params: dict[str, Any] | None) -> RunParams:
        """Build from Airflow DAG ``params`` (modo/periodo/estado)."""
        params = params or {}
        modo = str(params.get("modo", "smoke")).strip().lower()
        if modo not in ("smoke", "full"):
            raise ValueError(f"modo inválido: {modo}")
        periodo = ""
        if str(params.get("periodo", "")).strip():
            periodo = validate_periodo(str(params["periodo"]))
        if modo == "smoke":
            return cls(modo=modo, periodo=periodo, ufs=tuple(SMOKE_UFS), datasets=tuple(SMOKE_DATASETS))
        return cls(
            modo=modo,
            periodo=periodo,
            ufs=tuple(resolve_ufs(params)),
            datasets=tuple(FULL_DATASETS),
        )
