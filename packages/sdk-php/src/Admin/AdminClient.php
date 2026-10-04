<?php

declare(strict_types=1);

namespace Ankusa\Admin;

use Ankusa\Internal\HttpTransport;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * Operate against the operator API (`admin.port`, default 4002) —
 * `Ankusa.Admin.Router`: health, Prometheus metrics, the redacted
 * configuration, the dead-letter queue, replay jobs, and the quarantine list.
 * Responses are node-local by design, so a fleet operator scrapes every node's
 * admin port.
 *
 * The listener itself performs no authentication — `$headers` is for whatever a
 * deployer's own boundary (service mesh, an API gateway) expects in front of it.
 */
final class AdminClient
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
     * Liveness probe: `GET /health` -> `{status, instance, roles}`.
     *
     * @return array<array-key, mixed>
     */
    public function health(): array
    {
        return $this->json($this->request('GET', '/health'));
    }

    /**
     * `GET /metrics` -> the Prometheus text exposition body.
     */
    public function metrics(): string
    {
        return self::raw($this->request('GET', '/metrics'));
    }

    /**
     * `GET /v1/config` -> the effective, redacted configuration.
     *
     * @return array<array-key, mixed>
     */
    public function config(): array
    {
        return $this->json($this->request('GET', '/v1/config'));
    }

    /**
     * `GET /v1/dlq` -> a page of dead-lettered hooks, newest first.
     *
     * `$params` may carry `source_id`, `since` and `limit`; `null` values are
     * omitted from the query string rather than sent as `=null`.
     *
     * @param array<string, scalar|null> $params
     *
     * @return array<array-key, mixed>
     */
    public function listDeadLetters(array $params = []): array
    {
        return $this->json($this->request('GET', '/v1/dlq', $params));
    }

    /**
     * `POST /v1/replays` -> the created (202) or already-active (200) replay
     * job.
     *
     * `$spec` is the JSON body verbatim: `kind` (`dlq`, `archive` or `quarantine`) plus its
     * filter keys, and the optional `rate`/`max_lag_ms`. Posting the same spec
     * twice while the job is running or paused returns that job, so a proxy
     * retry is idempotent. The body always goes out as the spec object, never
     * `[]`.
     *
     * @param array<string, mixed> $spec
     *
     * @return array<array-key, mixed>
     */
    public function createReplay(array $spec): array
    {
        return $this->json($this->request('POST', '/v1/replays', json: $spec));
    }

    /**
     * `GET /v1/replays/{id}` -> the replay job.
     *
     * A missing job is a `404 replay_not_found` -> {@see AdminRejectedError}.
     *
     * @return array<array-key, mixed>
     */
    public function getReplay(string $id): array
    {
        return $this->json($this->request('GET', '/v1/replays/' . rawurlencode($id)));
    }

    /**
     * `GET /v1/replays` -> `{replays: [...]}`, newest first.
     *
     * @return array<array-key, mixed>
     */
    public function listReplays(): array
    {
        return $this->json($this->request('GET', '/v1/replays'));
    }

    /**
     * `PATCH /v1/replays/{id}` -> the updated replay job.
     *
     * `$patch` may carry `state` (`running`/`paused`/`cancelled`),
     * `rate` and `max_lag_ms`. A `done`/`cancelled`/`failed` job is a
     * `409 replay_finished`; a missing one is a `404 replay_not_found`.
     *
     * @param array<string, mixed> $patch
     *
     * @return array<array-key, mixed>
     */
    public function updateReplay(string $id, array $patch): array
    {
        return $this->json($this->request('PATCH', '/v1/replays/' . rawurlencode($id), json: $patch));
    }

    /**
     * `GET /v1/quarantine` -> recent quarantined hooks, newest first.
     *
     * `$params` may carry `limit`; `null` values are omitted from the query
     * string rather than sent as `=null`.
     *
     * @param array<string, scalar|null> $params
     *
     * @return array<array-key, mixed>
     */
    public function listQuarantined(array $params = []): array
    {
        return $this->json($this->request('GET', '/v1/quarantine', $params));
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
            throw new AdminUnavailableError('admin listener unreachable: ' . $err->getMessage(), $err);
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
            throw new AdminUnavailableError('admin listener returned a non-JSON body (' . $response->getStatusCode() . ')');
        }

        return $data;
    }

    private function raiseForStatus(ResponseInterface $response): void
    {
        $status = $response->getStatusCode();
        if ($status >= 200 && $status < 300) {
            return;
        }

        $body = HttpTransport::errorBody(self::raw($response));
        $code = self::stringField($body, 'error');

        if ($status === 409 && $code === 'role_not_enabled') {
            throw new RoleNotEnabledError(
                'role not enabled on this node: ' . HttpTransport::describe(self::stringField($body, 'role')),
                self::stringField($body, 'role'),
            );
        }

        if ($status >= 400 && $status < 500) {
            throw new AdminRejectedError(
                "admin listener rejected the request ({$status}): " . HttpTransport::describe($body),
                $status,
                $code,
            );
        }

        // Everything else that isn't 2xx is retryable: 1xx, an unfollowed 3xx
        // redirect, and 5xx.
        throw new AdminUnavailableError("admin listener error ({$status}): " . HttpTransport::describe($body));
    }

    private static function stringField(mixed $body, string $key): ?string
    {
        $value = \is_array($body) ? ($body[$key] ?? null) : null;

        return \is_string($value) ? $value : null;
    }

    private static function raw(ResponseInterface $response): string
    {
        return (string) $response->getBody();
    }
}
