<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

use Ankusa\AnkusaException;

/**
 * A delivery whose Standard Webhooks signature does not verify.
 *
 * Never retryable: answer `401`; the sender retries with the same bytes.
 * `$errorCode` is `invalid_secret`, `missing_header`, `invalid_timestamp`,
 * `timestamp_out_of_tolerance` or `no_matching_signature`; `$field` names the
 * header at fault (`null` for `invalid_secret`). (`$errorCode`, not `$code`:
 * PHP reserves `code` for {@see \Throwable}.)
 */
final class InvalidSignatureError extends \UnexpectedValueException implements AnkusaException
{
    public function __construct(
        string $message,
        public readonly string $errorCode,
        public readonly ?string $field = null,
    ) {
        parent::__construct($message);
    }

    public function isRetryable(): bool
    {
        return false;
    }
}
