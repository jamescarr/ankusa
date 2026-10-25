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

    /**
     * Whether the node verified the hook and nothing flags it: `true` only for
     * status `ok` with `flagged` false. A flagged hook, a `failed` or `skipped`
     * one, and any other status are untrusted.
     */
    public function isTrusted(): bool
    {
        return $this->status === 'ok' && !$this->flagged;
    }
}
