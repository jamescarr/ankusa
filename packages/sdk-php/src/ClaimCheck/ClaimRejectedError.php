<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * The gateway rejected the request (`400` or any other non-404 `4xx`).
 *
 * `$status` is the HTTP status; `$body` is the gateway's parsed JSON body, or
 * the raw text when it wasn't JSON (`''` when empty).
 */
final class ClaimRejectedError extends ClaimCheckError
{
    public function __construct(
        string $message,
        public readonly int $status,
        public readonly mixed $body,
    ) {
        parent::__construct($message);
    }
}
