<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

use Ankusa\Internal\HttpTransport;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * Redeem claim-check refs against a deployment's claim-check gateway.
 *
 * The gateway itself does no authentication or authorization (see
 * https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md) — `$headers`
 * is for whatever a deployer's own boundary (service mesh, an API gateway)
 * expects in front of it.
 */
final class ClaimCheckClient
{
    private const string SHA256_PATTERN = '~\A[0-9a-f]{64}\z~';

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
     * Redeem a claim-check ref: fetch its bytes and verify them against
     * `$sha256` (the queue message's `sha256` field, 64-char lowercase hex)
     * before returning. The gateway does not check integrity itself — see
     * "Redeem a claim" in
     * https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md — so this
     * end-to-end check always runs here.
     *
     * @throws ClaimCheckError check `isRetryable()` to sort a failure into
     *                         dead-letter (false) or retry (true)
     */
    public function redeem(string $ref, string $sha256): string
    {
        $parsed = ParsedClaimRef::parse($ref);
        if (preg_match(self::SHA256_PATTERN, $sha256) !== 1) {
            throw new InvalidClaimRefError('invalid claim sha256: ' . HttpTransport::describe($sha256));
        }

        $body = $this->fetchBytes($parsed);
        if (hash('sha256', $body) !== $sha256) {
            throw new ClaimIntegrityError("claim sha256 mismatch for {$parsed->tenantId}/{$parsed->claimId}");
        }

        return $body;
    }

    /**
     * Liveness probe: `GET /health`.
     *
     * @return array<array-key, mixed>
     */
    public function health(): array
    {
        try {
            $response = $this->http->send('GET', '/health');
        } catch (ClientExceptionInterface $err) {
            throw new ClaimCheckUnavailableError('claim-check gateway unreachable: ' . $err->getMessage(), $err);
        }

        $status = $response->getStatusCode();
        if ($status !== 200) {
            throw new ClaimCheckUnavailableError("claim-check gateway health check failed ({$status})");
        }

        $data = HttpTransport::decodeJson(self::raw($response));
        if ($data === null) {
            throw new ClaimCheckUnavailableError("claim-check gateway health check returned a non-JSON body ({$status})");
        }

        return $data;
    }

    private function fetchBytes(ParsedClaimRef $parsed): string
    {
        try {
            $response = $this->http->send('GET', $parsed->path);
        } catch (ClientExceptionInterface $err) {
            throw new ClaimCheckUnavailableError('claim-check gateway unreachable: ' . $err->getMessage(), $err);
        }

        $status = $response->getStatusCode();
        if ($status === 404) {
            throw new ClaimNotFoundError("claim not found: {$parsed->tenantId}/{$parsed->claimId}");
        }

        if ($status >= 400 && $status < 500) {
            $body = HttpTransport::errorBody(self::raw($response));

            throw new ClaimRejectedError(
                "claim-check rejected redeem ({$status}): " . HttpTransport::describe($body),
                $status,
                $body,
            );
        }

        if ($status !== 200) {
            throw new ClaimCheckUnavailableError(
                "claim-check gateway error ({$status}): " . HttpTransport::describe(HttpTransport::errorBody(self::raw($response))),
            );
        }

        return self::raw($response);
    }

    /** The whole body, from the start, whatever the stream's position. */
    private static function raw(ResponseInterface $response): string
    {
        return (string) $response->getBody();
    }
}
