<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

/**
 * How Ankusa verified the hook at ingest.
 */
final readonly class Verification
{
    public function __construct(
        /** `ok`, `failed` or `skipped`; other values pass through. */
        public string $status,
        /** `true` when a failed check was accepted (`on_verify_failure: accept_flag`): treat as unverified. */
        public bool $flagged,
    ) {}
}
