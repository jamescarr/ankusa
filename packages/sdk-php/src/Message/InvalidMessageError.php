<?php

declare(strict_types=1);

namespace Ankusa\Message;

use Ankusa\AnkusaException;

/**
 * A queue message failed to decode.
 *
 * Decoding is strict on purpose: a message is either the framework's own v1
 * payload or it isn't, so every failure here is a permanent one — never
 * requeue a message that raised this, dead-letter it.
 *
 * `$errorCode` is the machine-readable reason (`invalid_json`, `not_an_object`,
 * `unsupported_version`, `invalid_field`, `ambiguous_body`, `missing_body`,
 * `invalid_body_base64`, `size_mismatch`, `integrity`, `tenant_mismatch`);
 * `$field` names the offending key when the reason is `invalid_field` (a
 * `claim` that doesn't parse and a claim without a `sha256` use it too).
 * (`$errorCode`, not `$code`: PHP reserves `code` for {@see \Throwable}.)
 */
final class InvalidMessageError extends \UnexpectedValueException implements AnkusaException
{
    public function __construct(
        string $message,
        public readonly string $errorCode,
        public readonly ?string $field = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public function isRetryable(): bool
    {
        return false;
    }
}
