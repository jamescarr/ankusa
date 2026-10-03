<?php

declare(strict_types=1);

namespace Ankusa\Tests\Unit\Admin;

use Ankusa\Admin\AdminClient;
use Ankusa\Admin\AdminError;
use Ankusa\Admin\AdminUnavailableError;
use Ankusa\Admin\RoleNotEnabledError;
use Ankusa\Tests\Support\RecordingHttpClient;
use PHPUnit\Framework\TestCase;

/**
 * The admin vectors cover the status/body classification; this file covers the
 * wire shapes they cannot express: the spec a create posts, the patch an update
 * posts, the replay paths, the raw Prometheus body, query-parameter omission,
 * and the client headers that ride on every request.
 */
final class AdminClientTest extends TestCase
{
    private const string BASE_URL = 'http://admin.test';

    public function testCreateReplayPostsTheSpecAsTheBody(): void
    {
        $http = RecordingHttpClient::canned(202, [], '{"id":"r1","state":"running"}');
        $client = self::client($http);

        self::assertSame(['id' => 'r1', 'state' => 'running'], $client->createReplay(['kind' => 'dlq', 'source_id' => 'demo']));

        self::assertSame('{"kind":"dlq","source_id":"demo"}', self::body($http, 0));

        $request = self::requestAt($http, 0);
        self::assertSame('POST', $request['method']);
        self::assertSame('/v1/replays', $request['path']);
    }

    public function testGetListAndUpdateReplayUseTheReplayPaths(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"id":"r1","state":"paused"}');
        $client = self::client($http);

        $client->getReplay('0194f4a0-0000-7000-8000-0000000000aa');
        $client->listReplays();
        $client->updateReplay('0194f4a0-0000-7000-8000-0000000000aa', ['state' => 'paused']);

        self::assertSame('GET', self::requestAt($http, 0)['method']);
        self::assertSame('/v1/replays/0194f4a0-0000-7000-8000-0000000000aa', self::requestPath($http, 0));
        self::assertSame('GET', self::requestAt($http, 1)['method']);
        self::assertSame('/v1/replays', self::requestPath($http, 1));
        self::assertSame('PATCH', self::requestAt($http, 2)['method']);
        self::assertSame('/v1/replays/0194f4a0-0000-7000-8000-0000000000aa', self::requestPath($http, 2));
        self::assertSame('{"state":"paused"}', self::body($http, 2));
    }

    public function testMetricsReturnsTheRawNonJsonBodyVerbatim(): void
    {
        $metrics = "ankusa_ingest_requests_total{instance=\"default\"} 1\n";
        $http = RecordingHttpClient::canned(200, ['content-type' => 'text/plain; version=0.0.4'], $metrics);

        self::assertSame($metrics, self::client($http)->metrics());

        $request = self::requestAt($http, 0);
        self::assertSame('GET', $request['method']);
        self::assertSame('/metrics', $request['path']);
    }

    public function testJsonMethodAcceptsAJsonListBody(): void
    {
        $http = RecordingHttpClient::canned(200, [], '["edge","dispatch"]');

        self::assertSame(['edge', 'dispatch'], self::client($http)->health());
        self::assertSame('/health', self::requestPath($http, 0));
    }

    public function testRedirectIsUnavailable(): void
    {
        $http = RecordingHttpClient::canned(302, ['location' => 'http://elsewhere.test/health'], '');
        $client = self::client($http);

        $err = self::expectAdminError(AdminUnavailableError::class, static fn(): mixed => $client->health());

        self::assertTrue($err->isRetryable());
        self::assertStringContainsString('admin listener error (302)', $err->getMessage());
    }

    public function testNonJsonSuccessBodyIsUnavailable(): void
    {
        $http = RecordingHttpClient::canned(200, ['content-type' => 'text/plain'], 'not json');
        $client = self::client($http);

        $err = self::expectAdminError(AdminUnavailableError::class, static fn(): mixed => $client->health());

        self::assertTrue($err->isRetryable());
        self::assertStringContainsString('non-JSON body (200)', $err->getMessage());
    }

    public function testRoleNotEnabledCarriesTheRole(): void
    {
        $http = RecordingHttpClient::canned(409, [], '{"error":"role_not_enabled","role":"dispatch"}');
        $client = self::client($http);

        try {
            $client->listDeadLetters();
            self::fail('expected RoleNotEnabledError');
        } catch (RoleNotEnabledError $err) {
            self::assertSame('dispatch', $err->role);
            self::assertFalse($err->isRetryable());
        }
    }

    public function testQueryParamsAreSentOnlyWhenPresent(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"entries":[]}');
        $client = self::client($http);

        $client->listDeadLetters(['source_id' => 'demo', 'since' => 1_720_000_000_000, 'limit' => 10]);
        $client->listDeadLetters(['limit' => 5]);
        $client->listQuarantined(['limit' => 7]);
        $client->listQuarantined();

        self::assertSame('/v1/dlq?source_id=demo&since=1720000000000&limit=10', self::requestPath($http, 0));
        self::assertSame('/v1/dlq?limit=5', self::requestPath($http, 1));
        self::assertSame('/v1/quarantine?limit=7', self::requestPath($http, 2));
        self::assertSame('/v1/quarantine', self::requestPath($http, 3));
    }

    public function testClientHeadersRideOnEveryRequest(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"status":"ok"}');
        $client = new AdminClient(
            self::BASE_URL,
            ['X-Scope' => 'ops', 'authorization' => 'Bearer t'],
            httpClient: $http,
        );

        self::assertSame(['status' => 'ok'], $client->health());
        self::assertSame(['status' => 'ok'], $client->listDeadLetters());

        foreach ([0, 1] as $index) {
            $request = self::requestAt($http, $index);
            self::assertSame('ops', $request['headers']['x-scope']);
            self::assertSame('Bearer t', $request['headers']['authorization']);
        }

        $request = self::requestAt($http, 0);
        self::assertSame('GET', $request['method']);
        self::assertSame('/health', $request['path']);
        self::assertSame('/v1/dlq', self::requestPath($http, 1));
    }

    /* --- helpers ------------------------------------------------------------ */

    private static function client(RecordingHttpClient $http): AdminClient
    {
        return new AdminClient(self::BASE_URL, httpClient: $http);
    }

    /**
     * @param class-string<AdminError> $class
     */
    private static function expectAdminError(string $class, \Closure $call): AdminError
    {
        try {
            $call();
        } catch (AdminError $err) {
            self::assertSame($class, $err::class);

            return $err;
        }

        self::fail('expected ' . $class);
    }

    /**
     * @return array{method: string, path: string, headers: array<string, string>, body: string|null}
     */
    private static function requestAt(RecordingHttpClient $http, int $index): array
    {
        $requests = $http->requests();
        if (!isset($requests[$index])) {
            self::fail("no recorded request at index {$index}");
        }

        return $requests[$index];
    }

    private static function requestPath(RecordingHttpClient $http, int $index): string
    {
        return self::requestAt($http, $index)['path'];
    }

    private static function body(RecordingHttpClient $http, int $index): string
    {
        $body = self::requestAt($http, $index)['body'];
        if ($body === null) {
            self::fail("the request at index {$index} carried no body");
        }

        return $body;
    }
}
