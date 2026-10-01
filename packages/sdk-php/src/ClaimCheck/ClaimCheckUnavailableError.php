<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * The gateway is unreachable, or answered `5xx`/`503`. Safe to retry.
 *
 * A transport failure passes the PSR-18 client exception as `$previous`.
 */
final class ClaimCheckUnavailableError extends ClaimCheckError
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
