<?php

declare(strict_types=1);

namespace Ankusa\Sources;

use Ankusa\Internal\HttpTransport;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * Manage a deployment's tenant-scoped sources via the admin API
 * (`admin.port`, default 4002) — the `/v1/tenants/<tenant>/sources` endpoints
 * of `Ankusa.Admin.Router`.
 *
 * A tenant is the operator's own customer identifier and a source id is
 * `"<tenant>.<name>"`; this client speaks in those terms and builds the paths
 * for you. The API redacts secrets on read, so a {@see Source} is never useful
 * for editing — supply secrets through {@see SourceSpec}.
 *
 * Only `^[A-Za-z0-9_-]{1,64}$` tenants and names are accepted, and both are
 * checked before any path is built, so a caller-supplied name cannot escape its
 * tenant through URL normalization.
 *
 * `$expectedVersion` is an optional safety latch: when set, the first API call
 * fetches `GET /health` once, compares its `version` field against the expected
 * value, caches the fetched version, and raises {@see VersionMismatchError} on
 * any mismatch. Every subsequent call re-checks the cached value without
 * another request.
 */
final class SourcesClient
{
    /** The same rule the claim-check ref uses for its tenant. */
    private const string SAFE_ID = '~\A[A-Za-z0-9_-]{1,64}\z~';

    private readonly HttpTransport $http;

    private ?string $serverVersion = null;

    /**
     * @param string|null          $expectedVersion enforce `GET /health`'s `version` (once, cached)
     * @param float                $timeout         per-request timeout, in seconds; only used by the default transport
     * @param ClientInterface|null $httpClient      a PSR-18 client; an injected client owns its own timeouts and redirects
     */
    public function __construct(
        string $baseUrl,
        private readonly ?string $expectedVersion = null,
        float $timeout = 10.0,
        ?ClientInterface $httpClient = null,
    ) {
        $this->http = new HttpTransport($baseUrl, [], $timeout, $httpClient);
    }

    /**
     * The deployment's Ankusa version: `GET /health ["version"]`.
     *
     * Fetched once and cached; when `$expectedVersion` was set this also
     * enforces it, so a mismatched deployment raises `VersionMismatchError`
     * here too.
     *
     * @throws SourcesError
     */
    public function serverVersion(): string
    {
        $version = $this->serverVersion ??= $this->fetchVersion();
        $this->checkVersion();

        return $version;
    }

    /**
     * List a tenant's sources: `GET /v1/tenants/<tenant>/sources`.
     *
     * @return array<int, Source>
     *
     * @throws SourcesError
     */
    public function listSources(string $tenant): array
    {
        self::validateTenant($tenant);
        $this->ensureVersion();

        $response = $this->request('GET', "/v1/tenants/{$tenant}/sources");
        $this->raiseForStatus($response);

        $data = $this->decoded($response);
        $entries = $data['entries'] ?? null;
        if (!\is_array($entries)) {
            throw new SourcesUnavailableError(
                'ankusa admin API returned no entries (' . $response->getStatusCode() . '): ' . HttpTransport::describe($data),
            );
        }

        $sources = [];
        foreach ($entries as $entry) {
            $sources[] = Source::fromJson(\is_array($entry) ? $entry : []);
        }

        return $sources;
    }

    /**
     * Fetch one source: `GET /v1/tenants/<tenant>/sources/<name>`.
     *
     * @throws SourcesError
     */
    public function getSource(string $tenant, string $name): Source
    {
        self::validateTenant($tenant);
        self::validateName($name);
        $this->ensureVersion();

        $response = $this->request('GET', "/v1/tenants/{$tenant}/sources/{$name}");
        $this->raiseForStatus($response);

        return Source::fromJson($this->decoded($response));
    }

    /**
     * Create a source: `POST /v1/tenants/<tenant>/sources`.
     *
     * The source name travels in the body (plus `name`); the tenant comes from
     * the URL and wins over any `tenant` key inside the spec.
     *
     * @throws SourcesError
     */
    public function createSource(string $tenant, string $name, SourceSpec $spec): Source
    {
        self::validateTenant($tenant);
        self::validateName($name);
        $this->ensureVersion();

        $body = $spec->toJson();
        $body['name'] = $name;

        $response = $this->request('POST', "/v1/tenants/{$tenant}/sources", $body);
        $this->raiseForStatus($response);

        return Source::fromJson($this->decoded($response));
    }

    /**
     * Replace a source: `PUT /v1/tenants/<tenant>/sources/<name>`.
     *
     * The name comes from the URL; a `name` key inside the spec is never sent
     * (`SourceSpec` has no such field).
     *
     * @throws SourcesError
     */
    public function updateSource(string $tenant, string $name, SourceSpec $spec): Source
    {
        self::validateTenant($tenant);
        self::validateName($name);
        $this->ensureVersion();

        $response = $this->request('PUT', "/v1/tenants/{$tenant}/sources/{$name}", $spec->toJson());
        $this->raiseForStatus($response);

        return Source::fromJson($this->decoded($response));
    }

    /**
     * Delete a source: `DELETE /v1/tenants/<tenant>/sources/<name>`.
     *
     * Succeeds with no return value (the server answers `204` with an empty
     * body); a missing source raises {@see SourceNotFoundError}. A source that
     * still has undelivered hooks on the server's node raises
     * {@see SourceConflictError} whose body's `error` is
     * `source_has_deliveries` (with `pending`/`inflight` counts); this method
     * never sends the admin API's `?deliveries=dead_letter`.
     *
     * @throws SourcesError
     */
    public function deleteSource(string $tenant, string $name): void
    {
        self::validateTenant($tenant);
        self::validateName($name);
        $this->ensureVersion();

        $this->raiseForStatus($this->request('DELETE', "/v1/tenants/{$tenant}/sources/{$name}"));
    }

    /**
     * The optional version latch: only when `$expectedVersion` is set does the
     * first API call fetch `/health` once, cache the version, and enforce it.
     */
    private function ensureVersion(): void
    {
        if ($this->expectedVersion === null) {
            return;
        }

        if ($this->serverVersion === null) {
            $this->serverVersion = $this->fetchVersion();
        }
        $this->checkVersion();
    }

    private function checkVersion(): void
    {
        if ($this->expectedVersion !== null && $this->serverVersion !== $this->expectedVersion) {
            throw new VersionMismatchError(
                'expected Ankusa version ' . HttpTransport::describe($this->expectedVersion)
                . ', server reports ' . HttpTransport::describe($this->serverVersion),
            );
        }
    }

    private function fetchVersion(): string
    {
        $response = $this->request('GET', '/health');
        $status = $response->getStatusCode();

        if ($status !== 200) {
            $body = HttpTransport::errorBody(self::raw($response));

            throw new SourcesUnavailableError(
                "ankusa admin API health check failed ({$status}): " . HttpTransport::describe($body),
                $status,
                $body,
            );
        }

        $data = HttpTransport::decodeJson(self::raw($response));
        if ($data === null) {
            throw new SourcesUnavailableError("ankusa admin API health check returned a non-JSON body ({$status})");
        }

        $version = $data['version'] ?? null;
        if (!\is_string($version)) {
            throw new SourcesUnavailableError(
                "ankusa admin API health check returned no version ({$status}): " . HttpTransport::describe($data),
            );
        }

        return $version;
    }

    /**
     * @param array<string, mixed>|null $json
     */
    private function request(string $method, string $path, ?array $json = null): ResponseInterface
    {
        try {
            return $this->http->send($method, $path, json: $json);
        } catch (ClientExceptionInterface $err) {
            throw new SourcesUnavailableError('ankusa admin API unreachable: ' . $err->getMessage(), previous: $err);
        }
    }

    private function raiseForStatus(ResponseInterface $response): void
    {
        $status = $response->getStatusCode();
        if ($status >= 200 && $status < 300) {
            return;
        }

        $body = HttpTransport::errorBody(self::raw($response));

        if ($status === 404) {
            throw new SourceNotFoundError("source not found ({$status}): " . HttpTransport::describe($body), $status, $body);
        }

        if ($status === 400) {
            throw new SourceInvalidError(self::invalidMessage($body, $status), $status, $body);
        }

        if ($status === 409) {
            if (\is_array($body) && ($body['error'] ?? null) === 'source_store_read_only') {
                throw new SourceStoreReadOnlyError(
                    "source store is read-only ({$status}): " . HttpTransport::describe($body),
                    $status,
                    $body,
                );
            }

            throw new SourceConflictError("source already exists ({$status}): " . HttpTransport::describe($body), $status, $body);
        }

        throw new SourcesUnavailableError("ankusa admin API error ({$status}): " . HttpTransport::describe($body), $status, $body);
    }

    /**
     * @return array<array-key, mixed>
     */
    private function decoded(ResponseInterface $response): array
    {
        $data = HttpTransport::decodeJson(self::raw($response));
        if ($data === null) {
            throw new SourcesUnavailableError(
                'ankusa admin API returned a non-JSON body (' . $response->getStatusCode() . ')',
            );
        }

        return $data;
    }

    private static function invalidMessage(mixed $body, int $status): string
    {
        if (\is_array($body)) {
            $message = $body['message'] ?? $body['error'] ?? null;
            if (\is_string($message)) {
                return $message;
            }
        }

        return "invalid source ({$status}): " . HttpTransport::describe($body);
    }

    /**
     * Reject a tenant that is not `^[A-Za-z0-9_-]{1,64}$` before any path is
     * built.
     *
     * @throws SourceInvalidError
     */
    private static function validateTenant(string $tenant): void
    {
        if (preg_match(self::SAFE_ID, $tenant) !== 1) {
            throw new SourceInvalidError('invalid tenant: ' . HttpTransport::describe($tenant));
        }
    }

    /**
     * Reject a source name that is not `^[A-Za-z0-9_-]{1,64}$` before any path
     * is built.
     *
     * @throws SourceInvalidError
     */
    private static function validateName(string $name): void
    {
        if (preg_match(self::SAFE_ID, $name) !== 1) {
            throw new SourceInvalidError('invalid source name: ' . HttpTransport::describe($name));
        }
    }

    private static function raw(ResponseInterface $response): string
    {
        return (string) $response->getBody();
    }
}
