<?php

declare(strict_types=1);

namespace Ankusa\Admin;

/**
 * `GET /v1/audit` -> a page of the audit trail, newest first.
 */
final class AuditPage
{
    /**
     * @param list<AuditEntry> $entries
     */
    public function __construct(
        public readonly array $entries,
        /** Pass as `cursor` for the next page; `null` on the last. */
        public readonly ?string $nextCursor,
    ) {}

    /**
     * @param array<array-key, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $rawEntries = $data['entries'] ?? null;
        if (!\is_array($rawEntries)) {
            throw new AdminUnavailableError('admin listener returned a malformed audit page: entries is not an array');
        }

        $entries = [];
        foreach ($rawEntries as $entry) {
            if (!\is_array($entry)) {
                throw new AdminUnavailableError('admin listener returned a malformed audit page: an entry is not an object');
            }
            $entries[] = AuditEntry::fromArray($entry);
        }

        $nextCursor = $data['next_cursor'] ?? null;
        if ($nextCursor !== null && !\is_string($nextCursor)) {
            throw new AdminUnavailableError('admin listener returned a malformed audit page: next_cursor is not a string');
        }

        return new self($entries, $nextCursor);
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'entries' => array_map(static fn(AuditEntry $entry): array => $entry->toArray(), $this->entries),
            'next_cursor' => $this->nextCursor,
        ];
    }
}
