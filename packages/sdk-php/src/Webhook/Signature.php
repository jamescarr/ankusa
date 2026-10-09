<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

use Psr\Http\Message\MessageInterface;

/**
 * Verify the Standard Webhooks signature an HTTP sink with a `secret` adds
 * (https://www.standardwebhooks.com/).
 *
 * `webhook-signature` holds space-separated `v1,<base64>` entries, each an
 * HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. Any `v1` entry
 * matching any secret (`whsec_` + base64, or any other string used as its own
 * bytes) passes, compared with `hash_equals`, provided `webhook-timestamp` is
 * within the tolerance of now.
 */
final class Signature
{
    public const int DEFAULT_TOLERANCE_SECONDS = 300;

    private const array NAMES = ['webhook-id', 'webhook-timestamp', 'webhook-signature'];

    /**
     * Verify one delivery; `$body` must be the raw bytes received.
     *
     * @param MessageInterface|array<string, string|list<string>|null> $headers
     * @param string|list<string> $secrets
     *
     * @return array{id: string, timestamp: int}
     *
     * @throws InvalidSignatureError
     */
    public static function verify(
        MessageInterface|array $headers,
        string $body,
        string|array $secrets,
        int $toleranceSeconds = self::DEFAULT_TOLERANCE_SECONDS,
        ?int $now = null,
    ): array {
        $keys = self::keys(\is_string($secrets) ? [$secrets] : $secrets);

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

        $id = self::required($lowered, 'webhook-id');
        $rawTimestamp = self::required($lowered, 'webhook-timestamp');
        $signature = self::required($lowered, 'webhook-signature');

        if (preg_match('/\A[0-9]+\z/', $rawTimestamp) !== 1) {
            throw new InvalidSignatureError('webhook-timestamp is not a unix time', 'invalid_timestamp', 'webhook-timestamp');
        }
        $timestamp = (int) $rawTimestamp;
        if (abs(($now ?? time()) - $timestamp) > $toleranceSeconds) {
            throw new InvalidSignatureError(
                'webhook-timestamp is outside the tolerance window',
                'timestamp_out_of_tolerance',
                'webhook-timestamp',
            );
        }

        $signed = "{$id}.{$rawTimestamp}." . $body;
        $candidates = [];
        foreach (explode(' ', $signature) as $entry) {
            if (str_starts_with($entry, 'v1,')) {
                $candidates[] = substr($entry, 3);
            }
        }

        foreach ($keys as $key) {
            $expected = base64_encode(hash_hmac('sha256', $signed, $key, true));
            foreach ($candidates as $candidate) {
                if (hash_equals($expected, $candidate)) {
                    return ['id' => $id, 'timestamp' => $timestamp];
                }
            }
        }

        throw new InvalidSignatureError('no webhook-signature entry matches', 'no_matching_signature', 'webhook-signature');
    }

    /**
     * @param array<string, string|null> $lowered
     */
    private static function required(array $lowered, string $name): string
    {
        $value = $lowered[$name] ?? null;
        if ($value === null || $value === '') {
            throw new InvalidSignatureError("missing {$name} header", 'missing_header', $name);
        }

        return $value;
    }

    /**
     * @param list<string> $secrets
     *
     * @return list<string>
     */
    private static function keys(array $secrets): array
    {
        if ($secrets === []) {
            throw new InvalidSignatureError('no secret configured', 'invalid_secret');
        }

        $keys = [];
        foreach ($secrets as $secret) {
            if (str_starts_with($secret, 'whsec_')) {
                $key = base64_decode(substr($secret, 6), true);
                if ($key === false || $key === '') {
                    throw new InvalidSignatureError('a whsec_ secret is not valid base64', 'invalid_secret');
                }
                $keys[] = $key;
            } elseif ($secret === '') {
                throw new InvalidSignatureError('an empty secret', 'invalid_secret');
            } else {
                $keys[] = $secret;
            }
        }

        return $keys;
    }
}
