<?php

declare(strict_types=1);

namespace Ankusa\Admin;

/**
 * One row of the audit trail (`GET /v1/audit`), from `AuditEntry` in
 * `admin.v1.yaml`.
 */
final class AuditEntry
{
    public function __construct(
        /** The row's place in commit order. Strictly increasing; the key of the row. */
        public readonly int $seq,
        public readonly string $at,
        /** The principal id (`Ankusa.Principal`); `system` for a change made from code. */
        public readonly string $actor,
        /** The client address, behind `routes.trusted_proxies` the one `X-Forwarded-For` names. */
        public readonly ?string $remoteIp,
        public readonly string $action,
        public readonly ?string $tenant,
        /** The subject's id (`<tenant>.<name>` for a source, the route id, the replay job id). */
        public readonly ?string $resource,
        /** HMAC-SHA256 (hex) of the subject before the change; `null` when there was none. */
        public readonly ?string $beforeHash,
        /** HMAC-SHA256 (hex) of the subject after the change; `null` for a delete. */
        public readonly ?string $afterHash,
        /** The request's `X-Request-Id`, or the id the node made up. */
        public readonly ?string $requestId,
        /** `ok`, `failed` or `interrupted`. */
        public readonly string $result,
        public readonly ?int $revision,
        public readonly mixed $detail,
    ) {}

    /**
     * @param array<array-key, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            seq: self::int($data, 'seq'),
            at: self::string($data, 'at'),
            actor: self::string($data, 'actor'),
            remoteIp: self::nullableString($data, 'remote_ip'),
            action: self::string($data, 'action'),
            tenant: self::nullableString($data, 'tenant'),
            resource: self::nullableString($data, 'resource'),
            beforeHash: self::nullableString($data, 'before_hash'),
            afterHash: self::nullableString($data, 'after_hash'),
            requestId: self::nullableString($data, 'request_id'),
            result: self::string($data, 'result'),
            revision: self::nullableInt($data, 'revision'),
            detail: $data['detail'] ?? null,
        );
    }

    /**
     * The wire shape, field names and null handling exactly as the gateway
     * sends them.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'seq' => $this->seq,
            'at' => $this->at,
            'actor' => $this->actor,
            'remote_ip' => $this->remoteIp,
            'action' => $this->action,
            'tenant' => $this->tenant,
            'resource' => $this->resource,
            'before_hash' => $this->beforeHash,
            'after_hash' => $this->afterHash,
            'request_id' => $this->requestId,
            'result' => $this->result,
            'revision' => $this->revision,
            'detail' => $this->detail,
        ];
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function string(array $data, string $key): string
    {
        $value = $data[$key] ?? null;
        if (!\is_string($value)) {
            throw new AdminUnavailableError("admin listener returned a malformed audit entry: {$key} is not a string");
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function nullableString(array $data, string $key): ?string
    {
        $value = $data[$key] ?? null;
        if ($value !== null && !\is_string($value)) {
            throw new AdminUnavailableError("admin listener returned a malformed audit entry: {$key} is not a string");
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function int(array $data, string $key): int
    {
        $value = $data[$key] ?? null;
        if (!\is_int($value)) {
            throw new AdminUnavailableError("admin listener returned a malformed audit entry: {$key} is not an integer");
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function nullableInt(array $data, string $key): ?int
    {
        $value = $data[$key] ?? null;
        if ($value !== null && !\is_int($value)) {
            throw new AdminUnavailableError("admin listener returned a malformed audit entry: {$key} is not an integer");
        }

        return $value;
    }
}
