<?php

declare(strict_types=1);

namespace Ankusa\Admin;

/**
 * The listener rejected the request (any other `4xx`).
 *
 * `$status` is the HTTP status; `$errorCode` is the body's `error` field
 * (`invalid_filter`, ...). (`$errorCode`, not `code`: PHP reserves `code` for
 * {@see \Throwable}.)
 */
final class AdminRejectedError extends AdminError
{
    public function __construct(
        string $message,
        public readonly int $status,
        public readonly ?string $errorCode,
    ) {
        parent::__construct($message);
    }
}
