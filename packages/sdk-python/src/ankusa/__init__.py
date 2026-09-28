"""The ``ankusa`` PyPI package: everything a non-Elixir consumer needs to talk
to an Ankusa deployment. Today that's the claim-check gateway client
(``ankusa.claim_check``), the route-management client (``ankusa.routes``), the
operator client (``ankusa.admin``), and a webhook-receiving header helper
(``ankusa.webhook``); more clients (ingest) land here as they're built.
"""

from .admin import AdminClient, AdminError, AdminRejectedError, AdminUnavailableError, RoleNotEnabledError
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
from .routes import RouteNotFoundError, RoutesClient, RoutesError, RoutesRejectedError, RoutesUnavailableError
from .webhook import HookHeaders, MissingHookIdError, parse_headers

__all__ = [
    "AdminClient",
    "AdminError",
    "AdminUnavailableError",
    "RoleNotEnabledError",
    "AdminRejectedError",
    "ClaimCheckClient",
    "ClaimCheckError",
    "ClaimCheckUnavailableError",
    "ClaimIntegrityError",
    "ClaimNotFoundError",
    "ClaimRejectedError",
    "InvalidClaimRefError",
    "ParsedClaimRef",
    "parse_claim_ref",
    "RoutesClient",
    "RoutesError",
    "RoutesUnavailableError",
    "RouteNotFoundError",
    "RoutesRejectedError",
    "HookHeaders",
    "MissingHookIdError",
    "parse_headers",
]
