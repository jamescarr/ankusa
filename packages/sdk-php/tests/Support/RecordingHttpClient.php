<?php

declare(strict_types=1);

namespace Ankusa\Tests\Support;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * A PSR-18 client that serves canned responses in-process and records what it
 * was asked to send: the PHP stand-in for `httpx.MockTransport`, so the
 * conformance runner's `"transport": "injected"` cases and the unit tests never
 * need a socket.
 *
 * Records, per request: the method, the path with its query string (exactly as
 * the SDK built it), the headers lowercased to their first value, and the raw
 * body (`null` when empty). `requests()` returns those in order; assert on them
 * rather than on the `RequestInterface` the handler sees, whose body stream has
 * already been read.
 *
 * @internal
 */
final class RecordingHttpClient implements ClientInterface
{
    /** @var list<array{method: string, path: string, headers: array<string, string>, body: string|null}> */
    private array $requests = [];

    /**
     * @param \Closure(RequestInterface): ResponseInterface $handler
     */
    public function __construct(private readonly \Closure $handler) {}

    /**
     * Always answers with the same response and records the request.
     *
     * @param array<string, string> $headers
     */
    public static function canned(int $status, array $headers = [], string $payload = ''): self
    {
        $response = new Response($status, $headers + ['content-length' => (string) \strlen($payload)], $payload);

        return new self(static fn(RequestInterface $request): ResponseInterface => $response);
    }

    /**
     * Never answers: every request fails the way an unreachable server does.
     */
    public static function unreachable(string $message = 'connection refused'): self
    {
        return new self(static fn(RequestInterface $request): ResponseInterface => throw new ConnectException($message, $request));
    }

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $uri = $request->getUri();
        $query = $uri->getQuery();
        $body = (string) $request->getBody();

        $this->requests[] = [
            'method' => $request->getMethod(),
            'path' => $uri->getPath() . ($query === '' ? '' : '?' . $query),
            'headers' => self::firstValues($request->getHeaders()),
            'body' => $body === '' ? null : $body,
        ];

        return ($this->handler)($request);
    }

    /**
     * @return list<array{method: string, path: string, headers: array<string, string>, body: string|null}>
     */
    public function requests(): array
    {
        return $this->requests;
    }

    public function count(): int
    {
        return \count($this->requests);
    }

    /**
     * @param array<array-key, mixed> $headers
     *
     * @return array<string, string>
     */
    private static function firstValues(array $headers): array
    {
        $flattened = [];
        foreach ($headers as $name => $values) {
            $first = \is_array($values) ? ($values[0] ?? null) : $values;
            $flattened[strtolower((string) $name)] = \is_string($first) ? $first : '';
        }

        return $flattened;
    }
}
