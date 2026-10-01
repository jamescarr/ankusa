<?php

declare(strict_types=1);

namespace Ankusa\Routes;

/**
 * The listener rejected the request (`400` or any other non-404 `4xx`).
 *
 * `$errorCode` is the body's `error` field (`invalid_route`,
 * `duplicate_route`, `too_many_routes`, ...); `$field`, `$detail`,
 * `$conflictingId` and `$maxRoutes` are carried through when the body supplies
 * them with the right type. (`$detail` is the body's `message` field: PHP
 * reserves `message` for {@see \Throwable}.)
 */
final class RoutesRejectedError extends RoutesError
{
    public function __construct(
        string $message,
        public readonly int $status,
        public readonly ?string $errorCode,
        public readonly ?string $field = null,
        public readonly ?string $detail = null,
        public readonly ?string $conflictingId = null,
        public readonly ?int $maxRoutes = null,
    ) {
        parent::__construct($message);
    }
}
