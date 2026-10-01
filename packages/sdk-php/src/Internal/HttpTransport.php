<?php

declare(strict_types=1);

namespace Ankusa\Internal;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * The HTTP layer every client in this package shares.
 *
 * `@internal` — not part of the supported surface: it exists so the clients
 * take the same path whether they use the default transport or a caller's
 * PSR-18 client, and so query/body encoding is written down once.
 *
 * The default client never follows redirects (Guzzle's PSR-18 `sendRequest`
 * forces that) and never throws on a status code, so every non-2xx reaches the
 * caller's own classification. A caller-supplied client owns its own timeout
 * and redirect policy; the clients still classify any 3xx as unavailable.
 */
final class HttpTransport
{
    private const int JSON_FLAGS = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE;

    private readonly ClientInterface $http;

    private readonly string $baseUrl;

    /**
     * @param array<string, string> $headers sent on every request
     * @param float                 $timeout per-request timeout, in seconds; only used by the default client
     */
    public function __construct(
        string $baseUrl,
        private readonly array $headers = [],
        float $timeout = 10.0,
        ?ClientInterface $httpClient = null,
    ) {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->http = $httpClient ?? new Client([
            'timeout' => $timeout,
            'connect_timeout' => $timeout,
            'allow_redirects' => false,
            'http_errors' => false,
        ]);
    }

    /**
     * @param array<string, scalar|null> $query sent in input order; `null` values are dropped
     * @param array<string, mixed>|null  $json  encoded as a JSON object when not null
     *
     * @throws \Psr\Http\Client\ClientExceptionInterface when the request never completed
     */
    public function send(string $method, string $path, array $query = [], ?array $json = null): ResponseInterface
    {
        $headers = $this->headers;
        $body = '';
        if ($json !== null) {
            // Header names are case-insensitive: drop any caller spelling of
            // content-type first, or PSR-7 merges both into one header line.
            $headers = array_filter($headers, static fn(string $name): bool => strtolower($name) !== 'content-type', ARRAY_FILTER_USE_KEY);
            $headers['content-type'] = 'application/json';
            // A top-level request body is always a JSON object: an empty array
            // must go out as `{}`, not `[]`. Nested empty objects are the
            // caller's job (`(object) []`).
            $body = self::encode($json === [] ? new \stdClass() : $json);
        }

        return $this->http->sendRequest(new Request($method, $this->baseUrl . $path . self::queryString($query), $headers, $body));
    }

    /**
     * Decoded JSON (assoc) when the body parses as a JSON object or array,
     * else null — never throws.
     *
     * @return array<array-key, mixed>|null
     */
    public static function decodeJson(string $raw): ?array
    {
        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }

        return \is_array($decoded) ? $decoded : null;
    }

    /**
     * The mirror of Python's `_error_body`: the parsed JSON when the body is
     * valid JSON, else the raw text ('' when empty). Only used for error
     * payloads and messages, never for success bodies.
     */
    public static function errorBody(string $raw): mixed
    {
        try {
            return json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return $raw;
        }
    }

    /**
     * Best-effort JSON rendering of a value for an error message: the stand-in
     * for Python's `{body!r}` interpolation. Invalid UTF-8 is substituted
     * rather than failing the encode.
     */
    public static function describe(mixed $body): string
    {
        $json = json_encode($body, self::JSON_FLAGS | JSON_INVALID_UTF8_SUBSTITUTE);

        return $json === false ? '<unencodable>' : $json;
    }

    /**
     * `http_build_query` is unusable here: it emits `1`/`0` for booleans and
     * `+` for spaces. This keeps input order, drops nulls, and percent-encodes
     * both sides of every pair.
     *
     * @param array<string, scalar|null> $query
     */
    private static function queryString(array $query): string
    {
        $parts = [];
        foreach ($query as $key => $value) {
            if ($value === null) {
                continue;
            }
            $parts[] = rawurlencode($key) . '=' . rawurlencode(self::scalar($value));
        }

        return $parts === [] ? '' : '?' . implode('&', $parts);
    }

    private static function scalar(bool|float|int|string $value): string
    {
        return match ($value) {
            true => 'true',
            false => 'false',
            default => (string) $value,
        };
    }

    private static function encode(mixed $body): string
    {
        try {
            return json_encode($body, self::JSON_FLAGS | JSON_THROW_ON_ERROR);
        } catch (\JsonException $err) {
            throw new \InvalidArgumentException('request body is not JSON-encodable: ' . $err->getMessage(), 0, $err);
        }
    }
}
