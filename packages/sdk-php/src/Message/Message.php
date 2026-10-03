<?php

declare(strict_types=1);

namespace Ankusa\Message;

use Ankusa\ClaimCheck\InvalidClaimRefError;
use Ankusa\ClaimCheck\ParsedClaimRef;

/**
 * One decoded v1 queue message — what an Ankusa consumer reads off Redis, a
 * broker, or the HTTP sink.
 *
 * {@see self::decode()} is the inverse of `Ankusa.Sink.Message.encode/3`. It
 * verifies the body against `size` and `sha256` before handing anything back,
 * so a message that decodes is a message a consumer may act on. Every failure
 * raises {@see InvalidMessageError}.
 *
 * ```php
 * $message = Message::decode($payload);
 * if ($message->body === null) {
 *     $body = $claimCheck->redeem($message->claim, $message->sha256);
 * } else {
 *     $body = $message->body;
 * }
 * $key = $message->idempotencyKey();
 * ```
 *
 * The rules, in order, first failure wins:
 *
 * 1. not JSON -> `invalid_json`; JSON that isn't an object -> `not_an_object`.
 * 2. `v` missing or not the integer 1 -> `unsupported_version`.
 * 3. field types -> `invalid_field` with `field` set to the key: `id` a
 *    non-empty string; `source_id` a string; `received_at` an integer; `size`
 *    an integer >= 0; `tenant_id`/`content_type`/`dedupe_key`/`replay_id` a
 *    string, null, or absent; `headers` absent or an object of strings;
 *    `sha256` absent or 64 lowercase hex characters.
 * 4. body form: both `body_base64` and `claim` -> `ambiguous_body`; neither ->
 *    `missing_body`; invalid base64 -> `invalid_body_base64`; an unparsable
 *    `claim` -> `invalid_field` (`claim`); a `claim` without `sha256` ->
 *    `invalid_field` (`sha256`).
 * 5. inline: decoded length != `size` -> `size_mismatch`; a `sha256` that
 *    doesn't match the body -> `integrity`.
 * 6. claim: `tenant_id` set and different from the claim's tenant ->
 *    `tenant_mismatch`.
 *
 * Unknown keys are ignored. Absent `dedupe_key`/`replay_id`/`sha256` are
 * `null`; absent `headers` is `[]`.
 */
final readonly class Message
{
    /**
     * @param array<string, string> $headers forwarded provider headers, lowercased
     */
    public function __construct(
        public int $v,
        public string $id,
        public string $sourceId,
        public ?string $tenantId,
        public int $receivedAt,
        public ?string $contentType,
        public int $size,
        /** The raw body bytes; `null` when the body is behind a claim. */
        public ?string $body,
        public ?string $claim,
        public ?string $sha256,
        public ?string $dedupeKey,
        public ?string $replayId,
        public array $headers,
    ) {}

    /**
     * @throws InvalidMessageError always worth dead-lettering, never a retry
     */
    public static function decode(string $data): self
    {
        try {
            $raw = json_decode($data, false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $err) {
            throw new InvalidMessageError('message is not valid JSON', 'invalid_json', null, $err);
        }

        if (!$raw instanceof \stdClass) {
            throw new InvalidMessageError('message is not a JSON object', 'not_an_object');
        }

        $v = self::field($raw, 'v');
        if (!\is_int($v) || $v !== 1) {
            throw new InvalidMessageError('unsupported message version', 'unsupported_version');
        }

        $id = self::field($raw, 'id');
        if (!\is_string($id) || $id === '') {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'id');
        }

        $sourceId = self::field($raw, 'source_id');
        if (!\is_string($sourceId)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'source_id');
        }

        $receivedAt = self::field($raw, 'received_at');
        if (!\is_int($receivedAt)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'received_at');
        }

        $size = self::field($raw, 'size');
        if (!\is_int($size) || $size < 0) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'size');
        }

        $tenantId = self::optionalString($raw, 'tenant_id');
        $contentType = self::optionalString($raw, 'content_type');
        $dedupeKey = self::optionalString($raw, 'dedupe_key');
        $replayId = self::optionalString($raw, 'replay_id');

        $headers = self::headers($raw);

        $sha256 = self::field($raw, 'sha256');
        if ($sha256 !== null && (!\is_string($sha256) || preg_match('~\A[0-9a-f]{64}\z~', $sha256) !== 1)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'sha256');
        }

        $bodyBase64 = self::field($raw, 'body_base64');
        $claim = self::field($raw, 'claim');

        if ($bodyBase64 !== null && $claim !== null) {
            throw new InvalidMessageError('message has both body_base64 and claim', 'ambiguous_body');
        }
        if ($bodyBase64 === null && $claim === null) {
            throw new InvalidMessageError('message has neither body_base64 nor claim', 'missing_body');
        }

        if ($bodyBase64 !== null) {
            $body = \is_string($bodyBase64) ? base64_decode($bodyBase64, true) : false;
            if ($body === false) {
                throw new InvalidMessageError('body_base64 is not valid base64', 'invalid_body_base64');
            }
            if (\strlen($body) !== $size) {
                throw new InvalidMessageError('decoded body size does not match size', 'size_mismatch');
            }
            if ($sha256 !== null && hash('sha256', $body) !== $sha256) {
                throw new InvalidMessageError('body does not match sha256', 'integrity');
            }

            return new self(
                v: $v,
                id: $id,
                sourceId: $sourceId,
                tenantId: $tenantId,
                receivedAt: $receivedAt,
                contentType: $contentType,
                size: $size,
                body: $body,
                claim: null,
                sha256: $sha256,
                dedupeKey: $dedupeKey,
                replayId: $replayId,
                headers: $headers,
            );
        }

        if (!\is_string($claim)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'claim');
        }
        try {
            $ref = ParsedClaimRef::parse($claim);
        } catch (InvalidClaimRefError $err) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'claim', $err);
        }
        if ($sha256 === null) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'sha256');
        }
        if ($tenantId !== null && $ref->tenantId !== $tenantId) {
            throw new InvalidMessageError('claim tenant does not match tenant_id', 'tenant_mismatch');
        }

        return new self(
            v: $v,
            id: $id,
            sourceId: $sourceId,
            tenantId: $tenantId,
            receivedAt: $receivedAt,
            contentType: $contentType,
            size: $size,
            body: null,
            claim: $claim,
            sha256: $sha256,
            dedupeKey: $dedupeKey,
            replayId: $replayId,
            headers: $headers,
        );
    }

    /**
     * The key a consumer dedupes on.
     *
     * `source_id:dedupe_key` when the message carries a non-empty
     * `dedupe_key`, else the `id`; with `$includeReplay` and a `replay_id`,
     * `#replay:<replay_id>` is appended. The default ignores replays, so an
     * ordinary consumer drops a replayed event it already processed; pass
     * `true` to reprocess them.
     */
    public function idempotencyKey(bool $includeReplay = false): string
    {
        $key = ($this->dedupeKey !== null && $this->dedupeKey !== '')
            ? $this->sourceId . ':' . $this->dedupeKey
            : $this->id;

        if ($includeReplay && $this->replayId !== null) {
            $key .= '#replay:' . $this->replayId;
        }

        return $key;
    }

    private static function field(\stdClass $raw, string $key): mixed
    {
        return property_exists($raw, $key) ? $raw->{$key} : null;
    }

    private static function optionalString(\stdClass $raw, string $key): ?string
    {
        $value = self::field($raw, $key);
        if ($value === null) {
            return null;
        }
        if (!\is_string($value)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', $key);
        }

        return $value;
    }

    /**
     * @return array<string, string>
     */
    private static function headers(\stdClass $raw): array
    {
        $value = self::field($raw, 'headers');
        if ($value === null) {
            return [];
        }
        if (!$value instanceof \stdClass) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'headers');
        }

        $headers = [];
        foreach (get_object_vars($value) as $name => $item) {
            $headers[(string) $name] = self::headerValue($item);
        }

        return $headers;
    }

    private static function headerValue(mixed $item): string
    {
        if (!\is_string($item)) {
            throw new InvalidMessageError('invalid message field', 'invalid_field', 'headers');
        }

        return $item;
    }
}
