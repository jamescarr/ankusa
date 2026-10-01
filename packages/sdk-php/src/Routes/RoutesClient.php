<?php

declare(strict_types=1);

namespace Ankusa\Routes;

use Ankusa\Internal\HttpTransport;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * Manage routes and global IP rules on the route-management listener
 * (`routes.admin.port`, default 4003) — the `routes` tag of the framework's
 * `priv/openapi/admin.v1.yaml`.
 *
 * The listener itself performs no authentication — `$headers` is for whatever a
 * deployer's own boundary (service mesh, an API gateway) expects in front of it.
 *
 * Route ids are percent-encoded as a single path segment, so `/`, `?`, `#` and
 * `%` in an id can't reshape the URL; an empty id, or exactly `.` or `..`,
 * raises {@see InvalidRouteIdError} before any request is sent.
 */
final class RoutesClient
{
    private readonly HttpTransport $http;

    /**
     * @param array<string, string> $headers sent on every request
     * @param float                 $timeout per-request timeout, in seconds; only used by the default transport
     * @param ClientInterface|null  $httpClient a PSR-18 client; an injected client owns its own timeouts and redirects
     */
    public function __construct(
        string $baseUrl,
        array $headers = [],
        float $timeout = 10.0,
        ?ClientInterface $httpClient = null,
    ) {
        $this->http = new HttpTransport($baseUrl, $headers, $timeout, $httpClient);
    }

    /**
     * Liveness probe: `GET /health` -> `{status, routes}`.
     *
     * @return array<array-key, mixed>
     */
    public function health(): array
    {
        return $this->json($this->request('GET', '/health'));
    }

    /**
     * `GET /admin/routes` -> a page of route definitions.
     *
     * `$params` may carry `enabled`, `limit` and `cursor`; `null` values are
     * omitted from the query string rather than sent as `=null`.
     *
     * @param array<string, scalar|null> $params
     *
     * @return array<array-key, mixed>
     */
    public function listRoutes(array $params = []): array
    {
        return $this->json($this->request('GET', '/admin/routes', $params));
    }

    /**
     * `POST /admin/routes` -> the stored route, timestamps included.
     *
     * @param array<string, mixed> $input the route definition; the body carries
     *                                   exactly these keys
     *
     * @return array<array-key, mixed>
     */
    public function createRoute(array $input): array
    {
        return $this->json($this->request('POST', '/admin/routes', json: $input));
    }

    /**
     * `GET /admin/routes/{id}` -> the route.
     *
     * @return array<array-key, mixed>
     */
    public function getRoute(string $id): array
    {
        return $this->json($this->request('GET', '/admin/routes/' . self::routePath($id)));
    }

    /**
     * `PUT /admin/routes/{id}` -> the replaced (or created) route.
     *
     * @param array<string, mixed> $input
     *
     * @return array<array-key, mixed>
     */
    public function replaceRoute(string $id, array $input): array
    {
        return $this->json($this->request('PUT', '/admin/routes/' . self::routePath($id), json: $input));
    }

    /**
     * `PATCH /admin/routes/{id}` -> the patched route.
     *
     * @param array<string, mixed> $patch
     *
     * @return array<array-key, mixed>
     */
    public function updateRoute(string $id, array $patch): array
    {
        return $this->json($this->request('PATCH', '/admin/routes/' . self::routePath($id), json: $patch));
    }

    /**
     * `DELETE /admin/routes/{id}` (`204`, no body).
     */
    public function deleteRoute(string $id): void
    {
        $this->request('DELETE', '/admin/routes/' . self::routePath($id));
    }

    /**
     * `GET /admin/ip-rules` -> the global IP rules.
     *
     * @return array<array-key, mixed>
     */
    public function getIpRules(): array
    {
        return $this->json($this->request('GET', '/admin/ip-rules'));
    }

    /**
     * `PUT /admin/ip-rules` -> the stored rules, as parsed.
     *
     * @param array<string, mixed> $rules
     *
     * @return array<array-key, mixed>
     */
    public function putIpRules(array $rules): array
    {
        return $this->json($this->request('PUT', '/admin/ip-rules', json: $rules));
    }

    /**
     * `POST /admin/routes/test` -> the dry-run decision for `$request`.
     *
     * @param array<string, mixed> $request
     *
     * @return array<array-key, mixed>
     */
    public function testRoute(array $request): array
    {
        return $this->json($this->request('POST', '/admin/routes/test', json: $request));
    }

    /**
     * @param array<string, scalar|null> $params
     * @param array<string, mixed>|null  $json
     */
    private function request(string $method, string $path, array $params = [], ?array $json = null): ResponseInterface
    {
        try {
            $response = $this->http->send($method, $path, $params, $json);
        } catch (ClientExceptionInterface $err) {
            throw new RoutesUnavailableError('routes listener unreachable: ' . $err->getMessage(), $err);
        }

        $this->raiseForStatus($response);

        return $response;
    }

    /**
     * @return array<array-key, mixed>
     */
    private function json(ResponseInterface $response): array
    {
        $data = HttpTransport::decodeJson(self::raw($response));
        if ($data === null) {
            throw new RoutesUnavailableError('routes listener returned a non-JSON body (' . $response->getStatusCode() . ')');
        }

        return $data;
    }

    private function raiseForStatus(ResponseInterface $response): void
    {
        $status = $response->getStatusCode();
        if ($status >= 200 && $status < 300) {
            return;
        }

        if ($status === 404) {
            throw new RouteNotFoundError("route not found ({$status})");
        }

        if ($status >= 400 && $status < 500) {
            $body = HttpTransport::errorBody(self::raw($response));

            throw new RoutesRejectedError(
                "routes listener rejected the request ({$status}): " . HttpTransport::describe($body),
                $status,
                self::stringField($body, 'error'),
                self::stringField($body, 'field'),
                self::stringField($body, 'message'),
                self::stringField($body, 'conflicting_id'),
                self::intField($body, 'max_routes'),
            );
        }

        // Everything else that isn't 2xx is retryable: 1xx, an unfollowed 3xx
        // redirect, and 5xx.
        throw new RoutesUnavailableError(
            "routes listener error ({$status}): " . HttpTransport::describe(HttpTransport::errorBody(self::raw($response))),
        );
    }

    /**
     * Validate `$id` and percent-encode it as ONE path segment.
     *
     * `.` and `..` (and an empty id) are refused rather than encoded: a URL
     * parser normalizes them away before the request is sent, so `..` would
     * become `/admin/` and an empty id or `.` the collection endpoint — the
     * caller would get the list page back as if it were a route. Everything
     * else is `rawurlencode()`, so `/`, `?`, `#`, `%` and space travel as
     * escapes (`%2F %3F %23 %25 %20`) instead of reshaping the URL.
     */
    private static function routePath(string $id): string
    {
        if ($id === '' || $id === '.' || $id === '..') {
            throw new InvalidRouteIdError('invalid route id ' . HttpTransport::describe($id));
        }

        return rawurlencode($id);
    }

    private static function stringField(mixed $body, string $key): ?string
    {
        $value = \is_array($body) ? ($body[$key] ?? null) : null;

        return \is_string($value) ? $value : null;
    }

    private static function intField(mixed $body, string $key): ?int
    {
        $value = \is_array($body) ? ($body[$key] ?? null) : null;

        return \is_int($value) ? $value : null;
    }

    private static function raw(ResponseInterface $response): string
    {
        return (string) $response->getBody();
    }
}
