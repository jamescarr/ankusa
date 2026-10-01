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
 * source has a tenant), and `content-type`. A receiver must dedupe on
 * `x-ankusa-id`: delivery is at-least-once, so the same hook can arrive twice
 * after a retry.
 */
final readonly class HookHeaders
{
    private const array NAMES = ['x-ankusa-id', 'x-ankusa-source', 'x-ankusa-tenant', 'content-type'];

    public function __construct(
        public string $id,
        public string $source,
        /** Only present when the source has a tenant. */
        public ?string $tenant,
        public ?string $contentType,
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
        );
    }
}
