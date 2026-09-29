"""The ``ankusa`` PyPI package: everything a non-Elixir consumer needs to talk
to an Ankusa deployment. Today that's the claim-check gateway client
(``ankusa.claim_check``) and a webhook-receiving header helper
(``ankusa.webhook``); more clients (ingest, admin) land here as they're
built.
"""

from .admin import (
    AdminClient,
    AdminError,
    AdminUnavailableError,
    Source,
    SourceConflictError,
    SourceInvalidError,
    SourceNotFoundError,
    SourceSpec,
    SourceStoreReadOnlyError,
    VersionMismatchError,
)
from .claim_check import (
    ClaimCheckClient,
    ClaimCheckError,
    ClaimCheckUnavailableError,
    ClaimIntegrityError,
    ClaimNotFoundError,
    ClaimRejectedError,
    InvalidClaimRefError,
    ParsedClaimRef,
    parse_claim_ref,
)
from .webhook import HookHeaders, MissingHookIdError, parse_headers

__all__ = [
    "AdminClient",
    "AdminError",
    "AdminUnavailableError",
    "Source",
    "SourceConflictError",
    "SourceInvalidError",
    "SourceNotFoundError",
    "SourceSpec",
    "SourceStoreReadOnlyError",
    "VersionMismatchError",
    "ClaimCheckClient",
    "ClaimCheckError",
    "ClaimCheckUnavailableError",
    "ClaimIntegrityError",
    "ClaimNotFoundError",
    "ClaimRejectedError",
    "InvalidClaimRefError",
    "ParsedClaimRef",
    "parse_claim_ref",
    "HookHeaders",
    "MissingHookIdError",
    "parse_headers",
]
