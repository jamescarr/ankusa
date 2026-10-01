<?php

declare(strict_types=1);

namespace Ankusa\Sources;

use Ankusa\Internal\HttpTransport;

/**
 * A stored source as the admin API reports it: the redacted spec plus the
 * derived identity fields.
 *
 * Never useful for editing: the API redacts every secret (`secret`, `password`,
 * `token`, sink header values, URL userinfo passwords, ...), so resending a
 * read-back `$verify` map is not the same as resending the stored secret —
 * supply secrets through {@see SourceSpec}.
 */
final readonly class Source
{
    /**
     * @param array<string, mixed> $verify
     * @param array<int, mixed>    $sinks
     */
    public function __construct(
        public string $tenant,
        public string $name,
        public string $sourceId,
        public string $ingestPath,
        public array $verify,
        public ?string $onVerifyFailure,
        public array $sinks,
    ) {}

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws SourcesUnavailableError when the response isn't a source object
     */
    public static function fromJson(array $data): self
    {
        $verify = self::stringKeyed($data['verify'] ?? null);
        $sinks = $data['sinks'] ?? null;

        return new self(
            tenant: self::requiredString($data, 'tenant'),
            name: self::requiredString($data, 'name'),
            sourceId: self::requiredString($data, 'source_id'),
            ingestPath: self::requiredString($data, 'ingest_path'),
            verify: $verify === null || $verify === [] ? ['type' => 'none'] : $verify,
            onVerifyFailure: \is_string($data['on_verify_failure'] ?? null) ? $data['on_verify_failure'] : null,
            sinks: \is_array($sinks) ? array_values($sinks) : [],
        );
    }

    /**
     * @return array<string, mixed>|null null when the value isn't a JSON object
     */
    private static function stringKeyed(mixed $value): ?array
    {
        if (!\is_array($value)) {
            return null;
        }

        $object = [];
        foreach ($value as $key => $item) {
            if (!\is_string($key)) {
                return null;
            }
            $object[$key] = $item;
        }

        return $object;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function requiredString(array $data, string $key): string
    {
        $value = $data[$key] ?? null;
        if (!\is_string($value)) {
            throw new SourcesUnavailableError(
                "ankusa admin API returned a malformed source (no string '{$key}'): " . HttpTransport::describe($data),
            );
        }

        return $value;
    }
}
