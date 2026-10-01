<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * The sha256 of the bytes the gateway returned doesn't match the expected
 * sha256 the queue message carries. The gateway itself never checks this — see
 * "Redeem a claim" in
 * https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md — so this is
 * the reader's own end-to-end check, always run before `redeem()` returns.
 */
final class ClaimIntegrityError extends ClaimCheckError {}
