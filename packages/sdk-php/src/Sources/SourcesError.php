<?php

declare(strict_types=1);

namespace Ankusa\Sources;

use Ankusa\AnkusaException;

/**
 * Base for every error the source-management client raises.
 *
 * Every subclass carries the HTTP `$status` and decoded `$body` of the response
 * that produced it (both `null` when no response was involved, e.g. a transport
 * failure or an identifier rejected before any request).
 *
 * Non-retryable: `404` (no such source), `400` (a rejected spec, or a bad
 * tenant/name caught before the request), `409` (already exists, or a read-only
 * store), and a `GET /health` version mismatch. Retryable: the API is
 * unreachable, timed out, or answered `5xx`.
 */
abstract class SourcesError extends \RuntimeException implements AnkusaException
{
    public function __construct(
        string $message,
        public readonly ?int $status = null,
        public readonly mixed $body = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }
}
