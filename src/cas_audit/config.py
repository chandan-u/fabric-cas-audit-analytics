"""Configuration loading for the CAS audit analytics platform.

One pipeline definition runs in both local Spark and Fabric. The pipeline YAML
says *what* to do; the environment YAML says *where*. Notebooks never hardcode a
path -- they resolve it through here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Repo root, resolved from this file so it works from a notebook or pytest.
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = REPO_ROOT / "config"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def detect_env() -> str:
    """Return the environment name.

    Fabric notebooks get CAS_ENV injected by the Data Factory pipeline. When
    that is absent we look for the Fabric Spark runtime, then fall back to local.
    """
    if env := os.environ.get("CAS_ENV"):
        return env
    # Fabric sets this on every Spark pool node.
    if os.environ.get("AZURE_SERVICE") or os.environ.get("FABRIC_ENV_NAME"):
        return "fabric"
    return "local"


@dataclass
class EnvConfig:
    """Environment-specific settings: where data lives, how Spark is tuned."""

    env: str
    paths: dict[str, str]
    spark: dict[str, Any] = field(default_factory=dict)
    sampling: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, env: str | None = None) -> "EnvConfig":
        env = env or detect_env()
        raw = _read_yaml(CONFIG_ROOT / "env" / f"{env}.yaml")
        return cls(
            env=raw["env"],
            paths=raw.get("paths", {}),
            spark=raw.get("spark", {}),
            sampling=raw.get("sampling", {}),
        )

    def path(self, key: str, *parts: str) -> str:
        """Resolve a logical path key (``bronze``, ``files``, ...) plus subparts.

        Local paths are absolute so notebooks are cwd-independent. Fabric paths
        stay relative -- the attached Lakehouse resolves them.
        """
        if key not in self.paths:
            raise KeyError(f"Unknown path key '{key}'. Known: {sorted(self.paths)}")
        base = self.paths[key]
        if self.env == "local":
            base = str((REPO_ROOT / base).resolve())
        return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts if p]])

    @property
    def sampling_enabled(self) -> bool:
        return bool(self.sampling.get("enabled", False))

    @property
    def sampled_manufacturers(self) -> list[str]:
        return list(self.sampling.get("filter_manufacturers", []))


@dataclass
class PipelineConfig:
    """A single declarative pipeline step, loaded from config/pipelines/<name>.yaml."""

    name: str
    layer: str
    raw: dict[str, Any]

    @classmethod
    def load(cls, name: str) -> "PipelineConfig":
        raw = _read_yaml(CONFIG_ROOT / "pipelines" / f"{name}.yaml")
        return cls(name=raw.get("name", name), layer=raw.get("layer", ""), raw=raw)

    # Convenience accessors -- keep notebooks free of dict spelunking.
    @property
    def source(self) -> dict[str, Any]:
        return self.raw.get("source", {})

    @property
    def target(self) -> dict[str, Any]:
        return self.raw.get("target", {})

    @property
    def expectations(self) -> list[dict[str, Any]]:
        return self.raw.get("expectations", [])

    def get(self, key: str, default: Any = None) -> Any:
        """Fetch a pipeline setting.

        A YAML ``null`` means "not configured, use the default" -- without this,
        an explicit ``land_mode: null`` would override the caller's default with
        None and silently change behaviour.
        """
        value = self.raw.get(key)
        return default if value is None else value


def load(pipeline: str, env: str | None = None) -> tuple[EnvConfig, PipelineConfig]:
    """Load the (environment, pipeline) config pair a notebook needs."""
    return EnvConfig.load(env), PipelineConfig.load(pipeline)
