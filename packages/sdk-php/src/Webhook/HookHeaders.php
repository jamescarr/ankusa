<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

use Psr\Http\Message\MessageInterface;

/**
 * The identity of one HTTP-sink delivery.
 *
 * See "HTTP handoff" in
 * https://github.com/jamescarr/ankusa/blob/main/docs/integrations.md for the
 * full contract this mirrors: the raw body arrives verbatim, and identity
 * travels in `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-tenant` (only when the
 * source has a tenant), and `content-type`. A provider dedupe key
 * (`x-ankusa-dedupe-key`) and a replay marker (`x-ankusa-replay-id`) travel
 * alongside when the delivery has them.
 *
 * A receiver dedupes with {@see self::idempotencyKey()}, not `x-ankusa-id`
 * alone: delivery is at-least-once, so the same hook can arrive twice after a
 * retry, and a provider retry collapses onto the same `dedupe_key`.
 */
final readonly class HookHeaders
{
    private const array NAMES = [
        'x-ankusa-id',
        'x-ankusa-source',
        'x-ankusa-tenant',
        'x-ankusa-dedupe-key',
        'x-ankusa-replay-id',
        'content-type',
    ];

    public function __construct(
        public string $id,
        public string $source,
        /** Only present when the source has a tenant. */
        public ?string $tenant,
        public ?string $contentType,
        /** The provider event key; `null` when the delivery has none. */
        public ?string $dedupeKey,
        /** Set only when this delivery is a replay of an archived hook. */
        public ?string $replayId,
    ) {}

    /**
     * Parse the `x-ankusa-*` headers of one delivery.
     *
     * Accepts a PSR-7 request/response or a plain header array — PHP's
     * `getallheaders()`, PSR-7 `getHeaders()`, Symfony's `HeaderBag::all()` —
     * and looks the names up case-insensitively either way. Array values may
     * be a single string or a list (the first entry wins, `null` entries are
     * skipped).
     *
     * @param MessageInterface|array<string, string|list<string>|null> $headers
     *
     * @throws MissingHookIdError when `x-ankusa-id` is absent or empty
     */
    public static function fromHeaders(MessageInterface|array $headers): self
    {
        /** @var array<string, string|null> $lowered */
        $lowered = [];

        if ($headers instanceof MessageInterface) {
            foreach (self::NAMES as $name) {
                $lowered[$name] = $headers->getHeader($name)[0] ?? null;
            }
        } else {
            foreach ($headers as $name => $value) {
                $lowered[strtolower((string) $name)] = \is_array($value) ? ($value[0] ?? null) : $value;
            }
        }

        $id = $lowered['x-ankusa-id'] ?? null;
        if ($id === null || $id === '') {
            throw new MissingHookIdError('missing x-ankusa-id header');
        }

        return new self(
            id: $id,
            source: $lowered['x-ankusa-source'] ?? '',
            tenant: $lowered['x-ankusa-tenant'] ?? null,
            contentType: $lowered['content-type'] ?? null,
            dedupeKey: self::optional($lowered['x-ankusa-dedupe-key'] ?? null),
            replayId: self::optional($lowered['x-ankusa-replay-id'] ?? null),
        );
    }

    /**
     * The key a receiver dedupes on.
     *
     * `source:dedupe_key` when the delivery carries a non-empty
     * `x-ankusa-dedupe-key`, else the `id`; with `$includeReplay` and a
     * `x-ankusa-replay-id`, `#replay:<replay_id>` is appended. The default
     * ignores replays, so an ordinary receiver drops a replayed hook it
     * already processed; pass `true` to reprocess them.
     */
    public function idempotencyKey(bool $includeReplay = false): string
    {
        $key = ($this->dedupeKey !== null && $this->dedupeKey !== '')
            ? $this->source . ':' . $this->dedupeKey
            : $this->id;

        if ($includeReplay && $this->replayId !== null) {
            $key .= '#replay:' . $this->replayId;
        }

        return $key;
    }

    private static function optional(?string $value): ?string
    {
        return $value === null || $value === '' ? null : $value;
    }
}
