<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * The ref string isn't a `urn:ankusa:claim:v1:<tenant>:<claim_id>`
 * claim-check ref, or the expected sha256 isn't 64-char lowercase hex.
 */
final class InvalidClaimRefError extends ClaimCheckError {}
