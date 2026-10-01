<?php

declare(strict_types=1);

namespace Ankusa\Admin;

use Ankusa\AnkusaException;

/**
 * Base for every error the admin client raises.
 *
 * `isRetryable()` is the whole point of this hierarchy, exactly as in the
 * claim-check client: a caller (an operator script, a dashboard) needs one bit
 * — retry, or surface the rejection — and nothing here requires it to know the
 * operator API's status codes to get that right.
 *
 * Non-retryable: the listener said `409 role_not_enabled` (ask another node)
 * or another `4xx` (a rejected filter). Retryable: the listener said `5xx`, or
 * the request never completed (network error, timeout).
 */
abstract class AdminError extends \RuntimeException implements AnkusaException
{
    public function isRetryable(): bool
    {
        return false;
    }
}
