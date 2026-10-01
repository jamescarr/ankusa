<?php

declare(strict_types=1);

namespace Ankusa\Routes;

use Ankusa\AnkusaException;

/**
 * Base for every error the routes client raises.
 *
 * `isRetryable()` is the whole point of this hierarchy, exactly as in the
 * claim-check client: a caller (an operator script, a controller loop) needs
 * one bit — leave the table alone and retry, or surface the rejection — and
 * nothing here requires it to know the listener's status codes to get that
 * right.
 *
 * Non-retryable: the listener said `404` (no such route) or another `4xx`
 * (a rejected write, a duplicate, the cap), or the id was unusable before any
 * request was sent ({@see InvalidRouteIdError}). Retryable: the listener said
 * `5xx`/`503 store_unavailable`, or the request never completed (network
 * error, timeout).
 */
abstract class RoutesError extends \RuntimeException implements AnkusaException
{
    public function isRetryable(): bool
    {
        return false;
    }
}
