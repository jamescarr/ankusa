<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

use Ankusa\AnkusaException;

/**
 * Base for every error the claim-check client raises.
 *
 * `isRetryable()` is the whole point of this hierarchy: a caller (a queue
 * consumer, typically) needs exactly one bit — dead-letter or retry — and
 * nothing here requires it to know the gateway's status codes to get that
 * right.
 *
 * Non-retryable: the ref or expected sha256 is malformed, the gateway said
 * `404`/other `4xx`, or the bytes that came back don't match the sha256.
 * Retryable: the gateway said `5xx`/`503`, or the request never completed
 * (network error, timeout).
 */
abstract class ClaimCheckError extends \RuntimeException implements AnkusaException
{
    public function isRetryable(): bool
    {
        return false;
    }
}
