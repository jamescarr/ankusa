<?php

declare(strict_types=1);

namespace Ankusa\Admin;

/**
 * The listener is unreachable, or answered `5xx`. Safe to retry.
 *
 * A transport failure passes the PSR-18 client exception as `$previous`.
 */
final class AdminUnavailableError extends AdminError
{
    public function __construct(string $message, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    public function isRetryable(): bool
    {
        return true;
    }
}
