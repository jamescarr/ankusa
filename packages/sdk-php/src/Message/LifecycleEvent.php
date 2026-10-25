<?php

declare(strict_types=1);

namespace Ankusa\Message;

/**
 * One decoded lifecycle event: the CloudEvents 1.0 (structured mode) event an
 * Ankusa instance publishes when a source or a route is created, updated or
 * deleted, when a tenant is erased, or when the instance is restored from a
 * backup.
 *
 * It arrives as the body of a queue message from the instance's lifecycle
 * sinks (content type `application/cloudevents+json`, `source_id`
 * `ankusa:lifecycle`): decode the message with {@see Message::decode()},
 * redeem its body, and hand the bytes to {@see self::decode()}.
 *
 * Delivery is at least once and in commit order. `id` is derived from the
 * change, so the same event always has the same id, and `revision` (per
 * `subject`) lets a consumer ignore a stale update. `sequence` is the
 * instance's audit sequence number, the decimal string it is on the wire. An
 * optional field the event omitted or said `null` is `null`.
 *
 * The rules, in order, first failure wins ({@see InvalidMessageError}, never
 * retryable):
 *
 * 1. not JSON -> `invalid_json`; JSON that isn't an object -> `not_an_object`.
 * 2. `specversion` missing or not "1.0" -> `unsupported_version`.
 * 3. field types -> `invalid_field` with `field` set to the key: `id`,
 *    `source`, `type`, `subject`, `time` non-empty strings; `datacontenttype`
 *    a string, null, or absent; `data` an object, null, or absent; `revision`
 *    an integer >= 1, null, or absent; `actor`, `instance` a string, null, or
 *    absent; `sequence` a string of decimal digits, null, or absent;
 *    `replayid`, `traceparent`, `tracestate` a string, null, or absent.
 *
 * Unknown keys and unknown event types are ignored.
 */
final readonly class LifecycleEvent
{
    /**
     * @param array<string, mixed>|null $data the entity as the admin API shows it, secrets redacted
     */
    public function __construct(
        public string $specversion,
        public string $id,
        public string $source,
        /** `io.ankusa.source.created`, `io.ankusa.route.updated`, …; a type this SDK does not know passes through. */
        public string $type,
        public string $subject,
        /** When the change committed, as the wire says it (an RFC 3339 string, not parsed). */
        public string $time,
        public ?string $datacontenttype,
        public ?array $data,
        public ?int $revision,
        public ?string $actor,
        public ?string $instance,
        public ?string $sequence,
        public ?string $replayid,
        public ?string $traceparent,
        public ?string $tracestate,
    ) {}

    /**
     * @throws InvalidMessageError always worth dead-lettering, never a retry
     */
    public static function decode(string $data): self
    {
        try {
            $raw = json_decode($data, false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $err) {
            throw new InvalidMessageError('event is not valid JSON', 'invalid_json', null, $err);
        }

        if (!$raw instanceof \stdClass) {
            throw new InvalidMessageError('event is not a JSON object', 'not_an_object');
        }

        if (self::field($raw, 'specversion') !== '1.0') {
            throw new InvalidMessageError('unsupported specversion', 'unsupported_version');
        }

        $id = self::requiredString($raw, 'id');
        $source = self::requiredString($raw, 'source');
        $type = self::requiredString($raw, 'type');
        $subject = self::requiredString($raw, 'subject');
        $time = self::requiredString($raw, 'time');
        $datacontenttype = self::optionalString($raw, 'datacontenttype');

        $body = self::field($raw, 'data');
        if ($body !== null && !$body instanceof \stdClass) {
            throw new InvalidMessageError('invalid event field', 'invalid_field', 'data');
        }

        $revision = self::field($raw, 'revision');
        if ($revision !== null && (!\is_int($revision) || $revision < 1)) {
            throw new InvalidMessageError('invalid event field', 'invalid_field', 'revision');
        }

        $actor = self::optionalString($raw, 'actor');
        $instance = self::optionalString($raw, 'instance');

        $sequence = self::field($raw, 'sequence');
        if ($sequence !== null && (!\is_string($sequence) || preg_match('~\A[0-9]+\z~', $sequence) !== 1)) {
            throw new InvalidMessageError('invalid event field', 'invalid_field', 'sequence');
        }

        return new self(
            specversion: '1.0',
            id: $id,
            source: $source,
            type: $type,
            subject: $subject,
            time: $time,
            datacontenttype: $datacontenttype,
            data: $body === null ? null : self::toArray($body),
            revision: $revision,
            actor: $actor,
            instance: $instance,
            sequence: $sequence,
            replayid: self::optionalString($raw, 'replayid'),
            traceparent: self::optionalString($raw, 'traceparent'),
            tracestate: self::optionalString($raw, 'tracestate'),
        );
    }

    private static function field(\stdClass $raw, string $key): mixed
    {
        return property_exists($raw, $key) ? $raw->{$key} : null;
    }

    private static function requiredString(\stdClass $raw, string $key): string
    {
        $value = self::field($raw, $key);
        if (!\is_string($value) || $value === '') {
            throw new InvalidMessageError('invalid event field', 'invalid_field', $key);
        }

        return $value;
    }

    private static function optionalString(\stdClass $raw, string $key): ?string
    {
        $value = self::field($raw, $key);
        if ($value === null) {
            return null;
        }
        if (!\is_string($value)) {
            throw new InvalidMessageError('invalid event field', 'invalid_field', $key);
        }

        return $value;
    }

    /**
     * A decoded JSON object as the nested associative array a consumer reads.
     * An empty object is `[]`: PHP arrays cannot tell `{}` from `[]`.
     *
     * @return array<string, mixed>
     */
    private static function toArray(\stdClass $body): array
    {
        /** @var array<string, mixed> $array */
        $array = json_decode((string) json_encode($body), true, 512, JSON_THROW_ON_ERROR);

        return $array;
    }
}
