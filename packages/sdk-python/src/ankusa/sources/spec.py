"""The writable source spec and the redacted source the API returns."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

__all__ = ["SourceSpec", "Source"]


@dataclass(frozen=True)
class SourceSpec:
    """The writable fields of a source, as submitted to ``POST``/``PUT``.

    ``verify`` is optional (absent means ``{"type": "none"}`` on the server);
    ``sinks`` is required and must be non-empty -- the server validates all of
    this exactly as the YAML config does.
    """

    sinks: list[dict[str, Any]]
    verify: dict[str, Any] | None = None
    on_verify_failure: str | None = None

    def to_json(self) -> dict[str, Any]:
        """The JSON body for a create/update, omitting unset (``None``) fields."""
        body: dict[str, Any] = {"sinks": self.sinks}
        if self.verify is not None:
            body["verify"] = self.verify
        if self.on_verify_failure is not None:
            body["on_verify_failure"] = self.on_verify_failure
        return body


@dataclass(frozen=True)
class Source:
    """A stored source as the admin API reports it: redacted spec plus the
    derived identity fields."""

    tenant: str
    name: str
    source_id: str
    ingest_path: str
    verify: dict[str, Any]
    on_verify_failure: str | None
    sinks: list[dict[str, Any]]

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Source:
        return cls(
            tenant=data["tenant"],
            name=data["name"],
            source_id=data["source_id"],
            ingest_path=data["ingest_path"],
            verify=data.get("verify") or {"type": "none"},
            on_verify_failure=data.get("on_verify_failure"),
            sinks=list(data.get("sinks") or []),
        )

