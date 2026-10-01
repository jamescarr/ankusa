<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * A parsed claim-check ref, ready to become a `GET /v1/claims/...` request.
 */
final readonly class ParsedClaimRef
{
    /**
     * Mirrors `#/components/schemas/Ref` in the framework's
     * `priv/openapi/claim_check.v1.yaml` — keep the two in sync. `\A`/`\z`, not
     * `^`/`$`: PHP's `$` also matches before a trailing newline.
     */
    private const string PATTERN = '~\Aurn:ankusa:claim:v1:(?<tenant_id>[A-Za-z0-9_-]{1,64}):(?<claim_id>[0-7][0-9A-HJKMNP-TV-Z]{25})\z~';

    public function __construct(
        public string $tenantId,
        /** Canonical (uppercase) ULID. */
        public string $claimId,
        /** `/v1/claims/{tenant_id}/{claim_id}`. */
        public string $path,
    ) {}

    /**
     * @throws InvalidClaimRefError — never worth retrying — if `$ref` isn't a
     *                              well-formed ref
     */
    public static function parse(string $ref): self
    {
        if (preg_match(self::PATTERN, $ref, $matches) !== 1) {
            throw new InvalidClaimRefError("invalid claim-check ref: {$ref}");
        }

        $tenantId = (string) $matches['tenant_id'];
        $claimId = (string) $matches['claim_id'];

        return new self($tenantId, $claimId, "/v1/claims/{$tenantId}/{$claimId}");
    }
}
