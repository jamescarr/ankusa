<?php

declare(strict_types=1);

namespace Ankusa\Internal;

/**
 * The one idempotency-key rule, shared by `Message` and `HookHeaders`.
 *
 * `@internal` — not part of the supported surface: both public
 * `idempotencyKey()` helpers delegate here so the rule is written down once.
 *
 * The key Ankusa shipped (the message's `idempotency_key`, or the
 * `x-ankusa-idempotency-key` header) when it is a non-empty string. For a hook
 * from a node that predates the field it is computed:
 * `tenant:source_id:dedupe_key` (tenant `default` when there is none) for a
 * non-empty dedupe key, else the `id`. With `$includeReplay` and a replay id,
 * `#replay:<replay_id>` is appended.
 */
final class IdempotencyKey
{
    public static function build(
        ?string $shipped,
        ?string $tenant,
        string $source,
        ?string $dedupeKey,
        string $id,
        ?string $replayId,
        bool $includeReplay,
    ): string {
        if ($shipped !== null && $shipped !== '') {
            $key = $shipped;
        } elseif ($dedupeKey !== null && $dedupeKey !== '') {
            $key = ($tenant ?? 'default') . ':' . $source . ':' . $dedupeKey;
        } else {
            $key = $id;
        }

        if ($includeReplay && $replayId !== null) {
            $key .= '#replay:' . $replayId;
        }

        return $key;
    }
}
