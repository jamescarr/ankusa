<?php

declare(strict_types=1);

namespace Ankusa\Tests\Unit\Sources;

use Ankusa\Sources\Source;
use Ankusa\Sources\SourceConflictError;
use Ankusa\Sources\SourceInvalidError;
use Ankusa\Sources\SourceNotFoundError;
use Ankusa\Sources\SourcesClient;
use Ankusa\Sources\SourcesError;
use Ankusa\Sources\SourceSpec;
use Ankusa\Sources\SourceStoreReadOnlyError;
use Ankusa\Sources\SourcesUnavailableError;
use Ankusa\Sources\VersionMismatchError;
use Ankusa\Tests\Support\RecordingHttpClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * The source-management client has no conformance vectors, so this file is the
 * PHP port of `packages/sdk-python/tests/test_sources.py`: the paths and bodies
 * it puts on the wire, its error classification, and the version latch.
 */
final class SourcesClientTest extends TestCase
{
    private const string BASE_URL = 'http://admin.test';

    private const array ENTRY = [
        'tenant' => 'acme',
        'name' => 'billing',
        'source_id' => 'acme.billing',
        'ingest_path' => '/webhooks/acme.billing',
        'verify' => ['type' => 'hmac', 'secret' => '[REDACTED]', 'signature_header' => 'X-Sig'],
        'on_verify_failure' => 'reject',
        'sinks' => [['type' => 'log']],
    ];

    /* --- SourceSpec / Source ------------------------------------------------ */

    public function testSourceSpecToJsonKeepsEverySetField(): void
    {
        self::assertSame(
            [
                'sinks' => [['type' => 'log']],
                'verify' => ['type' => 'hmac', 'secret' => 's3cr3t', 'signature_header' => 'X-Sig'],
                'on_verify_failure' => 'reject',
            ],
            self::spec()->toJson(),
        );
    }

    public function testSourceSpecToJsonOmitsUnsetVerifyAndFailureMode(): void
    {
        self::assertSame(['sinks' => [['type' => 'log']]], (new SourceSpec(sinks: [['type' => 'log']]))->toJson());
    }

    public function testSourceFromJsonReadsTheSnakeCaseFields(): void
    {
        $source = Source::fromJson(self::ENTRY);

        self::assertSame('acme', $source->tenant);
        self::assertSame('billing', $source->name);
        self::assertSame('acme.billing', $source->sourceId);
        self::assertSame('/webhooks/acme.billing', $source->ingestPath);
        self::assertSame(['type' => 'hmac', 'secret' => '[REDACTED]', 'signature_header' => 'X-Sig'], $source->verify);
        self::assertSame('reject', $source->onVerifyFailure);
        self::assertSame([['type' => 'log']], $source->sinks);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function withoutVerifyOrSinks(): iterable
    {
        yield 'absent keys' => [[
            'tenant' => 'acme',
            'name' => 'billing',
            'source_id' => 'acme.billing',
            'ingest_path' => '/webhooks/acme.billing',
        ]];

        yield 'empty object and list' => [[
            'tenant' => 'acme',
            'name' => 'billing',
            'source_id' => 'acme.billing',
            'ingest_path' => '/webhooks/acme.billing',
            'verify' => [],
            'sinks' => [],
        ]];
    }

    /**
     * @param array<string, mixed> $data
     */
    #[DataProvider('withoutVerifyOrSinks')]
    public function testSourceFromJsonDefaultsVerifyAndSinks(array $data): void
    {
        $source = Source::fromJson($data);

        self::assertSame(['type' => 'none'], $source->verify);
        self::assertSame([], $source->sinks);
        self::assertNull($source->onVerifyFailure);
    }

    /* --- server version / version latch ------------------------------------- */

    public function testServerVersionReadsTheHealthVersion(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"status":"ok","version":"0.3.0"}');

        self::assertSame('0.3.0', self::client($http)->serverVersion());
        self::assertSame('/health', self::requestPath($http, 0));
    }

    public function testServerVersionFetchesHealthOnceAndCachesIt(): void
    {
        $http = self::healthOrSourcesHandler();
        $client = self::client($http);

        self::assertSame('0.3.0', $client->serverVersion());
        self::assertSame([], $client->listSources('acme'));
        self::assertSame(1, self::healthRequestCount($http));
    }

    public function testListSourcesRaisesVersionMismatchWhenExpectedVersionDiffers(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"status":"ok","version":"0.3.0"}');
        $client = self::client($http, expectedVersion: '9.9.9');

        $err = self::expectSourcesError(VersionMismatchError::class, static fn(): mixed => $client->listSources('acme'));

        self::assertStringContainsString('9.9.9', $err->getMessage());
        self::assertStringContainsString('0.3.0', $err->getMessage());
        self::assertNull($err->status);
        self::assertNull($err->body);
    }

    public function testVersionMismatchIsRecheckedFromCacheWithoutAnotherHealthRequest(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"status":"ok","version":"0.3.0"}');
        $client = self::client($http, expectedVersion: '9.9.9');

        self::expectSourcesError(VersionMismatchError::class, static fn(): mixed => $client->listSources('acme'));
        self::expectSourcesError(VersionMismatchError::class, static fn(): mixed => $client->getSource('acme', 'billing'));

        self::assertSame(1, self::healthRequestCount($http));
    }

    public function testExpectedVersionMatchDoesNotRaise(): void
    {
        $http = new RecordingHttpClient(static function (RequestInterface $request): ResponseInterface {
            if ($request->getUri()->getPath() === '/health') {
                return self::jsonResponse(200, ['status' => 'ok', 'version' => '0.3.0']);
            }

            return self::jsonResponse(200, ['tenant' => 'acme', 'entries' => [self::ENTRY]]);
        });

        self::assertEquals([Source::fromJson(self::ENTRY)], self::client($http, expectedVersion: '0.3.0')->listSources('acme'));
    }

    public function testHealthNon200IsUnavailable(): void
    {
        $http = RecordingHttpClient::canned(503, [], '{"error":"boom"}');

        $err = self::expectSourcesError(
            SourcesUnavailableError::class,
            static fn(): mixed => self::client($http)->serverVersion(),
        );

        self::assertSame(503, $err->status);
        self::assertSame(['error' => 'boom'], $err->body);
    }

    public function testHealthNonJsonBodyIsUnavailable(): void
    {
        $http = RecordingHttpClient::canned(200, [], 'not json');

        $err = self::expectSourcesError(
            SourcesUnavailableError::class,
            static fn(): mixed => self::client($http)->serverVersion(),
        );

        self::assertNull($err->status);
        self::assertNull($err->body);
        self::assertStringContainsString('non-JSON body (200)', $err->getMessage());
    }

    public function testHealthWithoutVersionIsUnavailable(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"status":"ok"}');

        $err = self::expectSourcesError(
            SourcesUnavailableError::class,
            static fn(): mixed => self::client($http)->serverVersion(),
        );

        self::assertNull($err->status);
        self::assertNull($err->body);
        self::assertStringContainsString('returned no version (200)', $err->getMessage());
    }

    /* --- list / get / create / update / delete ------------------------------ */

    public function testListSourcesParsesEntries(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"tenant":"acme","entries":[' . self::encode(self::ENTRY) . ']}');

        self::assertEquals([Source::fromJson(self::ENTRY)], self::client($http)->listSources('acme'));

        $request = self::requestAt($http, 0);
        self::assertSame('GET', $request['method']);
        self::assertSame('/v1/tenants/acme/sources', $request['path']);
        self::assertSame(1, $http->count());
    }

    public function testGetSourceRequiresNoVersionProbeWithoutExpectedVersion(): void
    {
        $http = RecordingHttpClient::canned(200, [], self::encode(self::ENTRY));

        $source = self::client($http)->getSource('acme', 'billing');

        self::assertSame('acme.billing', $source->sourceId);
        self::assertSame('/webhooks/acme.billing', $source->ingestPath);
        self::assertSame(['type' => 'hmac', 'secret' => '[REDACTED]', 'signature_header' => 'X-Sig'], $source->verify);
        self::assertSame('reject', $source->onVerifyFailure);
        self::assertSame([['type' => 'log']], $source->sinks);

        $request = self::requestAt($http, 0);
        self::assertSame('GET', $request['method']);
        self::assertSame('/v1/tenants/acme/sources/billing', $request['path']);
        self::assertSame(1, $http->count());
    }

    public function testCreateSourcePostsTheSpecPlusName(): void
    {
        $http = RecordingHttpClient::canned(201, [], self::encode(self::ENTRY));

        self::assertEquals(Source::fromJson(self::ENTRY), self::client($http)->createSource('acme', 'billing', self::spec()));

        $request = self::requestAt($http, 0);
        self::assertSame('POST', $request['method']);
        self::assertSame('/v1/tenants/acme/sources', $request['path']);
        self::assertSame(
            [
                'sinks' => [['type' => 'log']],
                'verify' => ['type' => 'hmac', 'secret' => 's3cr3t', 'signature_header' => 'X-Sig'],
                'on_verify_failure' => 'reject',
                'name' => 'billing',
            ],
            self::decodedBody($http, 0),
        );
    }

    public function testCreateSourceSendsNoVerifyWhenSpecHasNone(): void
    {
        $http = RecordingHttpClient::canned(201, [], self::encode(self::ENTRY));

        self::client($http)->createSource('acme', 'billing', new SourceSpec(sinks: [['type' => 'log']]));

        self::assertSame(['sinks' => [['type' => 'log']], 'name' => 'billing'], self::decodedBody($http, 0));
    }

    public function testUpdateSourcePutsToTheNamedPath(): void
    {
        $http = RecordingHttpClient::canned(200, [], self::encode(self::ENTRY));

        self::assertEquals(Source::fromJson(self::ENTRY), self::client($http)->updateSource('acme', 'billing', self::spec()));

        $request = self::requestAt($http, 0);
        self::assertSame('PUT', $request['method']);
        self::assertSame('/v1/tenants/acme/sources/billing', $request['path']);
        // The name lives in the URL: it is never repeated in the body.
        self::assertSame(
            [
                'sinks' => [['type' => 'log']],
                'verify' => ['type' => 'hmac', 'secret' => 's3cr3t', 'signature_header' => 'X-Sig'],
                'on_verify_failure' => 'reject',
            ],
            self::decodedBody($http, 0),
        );
    }

    public function testDeleteSourceSendsNoBodyOn204(): void
    {
        $http = RecordingHttpClient::canned(204);

        self::client($http)->deleteSource('acme', 'billing');

        $request = self::requestAt($http, 0);
        self::assertSame('DELETE', $request['method']);
        self::assertSame('/v1/tenants/acme/sources/billing', $request['path']);
        self::assertNull($request['body']);
    }

    /* --- error classification ---------------------------------------------- */

    public function testDeleteSource404MapsToSourceNotFoundError(): void
    {
        $http = RecordingHttpClient::canned(404, [], '{"error":"source_not_found"}');
        $client = self::client($http);

        $err = self::expectSourcesError(
            SourceNotFoundError::class,
            static function () use ($client): void {
                $client->deleteSource('acme', 'billing');
            },
        );

        self::assertSame(404, $err->status);
        self::assertSame(['error' => 'source_not_found'], $err->body);

        $request = self::requestAt($http, 0);
        self::assertSame('DELETE', $request['method']);
        self::assertSame('/v1/tenants/acme/sources/billing', $request['path']);
    }

    public function testDeleteSource409SourceStoreReadOnlyMapsToSourceStoreReadOnlyError(): void
    {
        $http = RecordingHttpClient::canned(409, [], '{"error":"source_store_read_only"}');
        $client = self::client($http);

        $err = self::expectSourcesError(
            SourceStoreReadOnlyError::class,
            static function () use ($client): void {
                $client->deleteSource('acme', 'billing');
            },
        );

        self::assertSame(409, $err->status);
        self::assertSame(['error' => 'source_store_read_only'], $err->body);
    }

    public function testDeleteSource400MapsToSourceInvalidError(): void
    {
        $http = RecordingHttpClient::canned(400, [], '{"error":"invalid_source","message":"cannot delete a seeded source"}');
        $client = self::client($http);

        $err = self::expectSourcesError(
            SourceInvalidError::class,
            static function () use ($client): void {
                $client->deleteSource('acme', 'billing');
            },
        );

        self::assertSame(400, $err->status);
        self::assertSame('cannot delete a seeded source', $err->getMessage());
        self::assertSame(['error' => 'invalid_source', 'message' => 'cannot delete a seeded source'], $err->body);
    }

    public function test404MapsToSourceNotFoundError(): void
    {
        $http = RecordingHttpClient::canned(404, [], '{"error":"source_not_found"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourceNotFoundError::class, static fn(): mixed => $client->getSource('acme', 'missing'));

        self::assertSame(404, $err->status);
        self::assertSame(['error' => 'source_not_found'], $err->body);
    }

    public function test409SourceExistsMapsToSourceConflictError(): void
    {
        $http = RecordingHttpClient::canned(409, [], '{"error":"source_exists"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourceConflictError::class, static fn(): mixed => $client->createSource('acme', 'billing', self::spec()));

        self::assertSame(409, $err->status);
        self::assertSame(['error' => 'source_exists'], $err->body);
    }

    public function test409SourceStoreReadOnlyMapsToSourceStoreReadOnlyError(): void
    {
        $http = RecordingHttpClient::canned(409, [], '{"error":"source_store_read_only"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourceStoreReadOnlyError::class, static fn(): mixed => $client->updateSource('acme', 'billing', self::spec()));

        self::assertSame(409, $err->status);
        self::assertSame(['error' => 'source_store_read_only'], $err->body);
    }

    public function test400WithMessageCarriesTheServerMessage(): void
    {
        $http = RecordingHttpClient::canned(400, [], '{"error":"invalid_source","message":"sinks must not be empty"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourceInvalidError::class, static fn(): mixed => $client->createSource('acme', 'billing', self::spec()));

        self::assertSame(400, $err->status);
        self::assertSame('sinks must not be empty', $err->getMessage());
        self::assertSame(['error' => 'invalid_source', 'message' => 'sinks must not be empty'], $err->body);
    }

    public function test400WithoutMessageCarriesTheErrorCode(): void
    {
        $http = RecordingHttpClient::canned(400, [], '{"error":"invalid_tenant"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourceInvalidError::class, static fn(): mixed => $client->listSources('acme'));

        self::assertSame(400, $err->status);
        self::assertSame('invalid_tenant', $err->getMessage());
        self::assertSame(['error' => 'invalid_tenant'], $err->body);
    }

    public function test5xxMapsToSourcesUnavailableError(): void
    {
        $http = RecordingHttpClient::canned(503, [], '{"error":"boom"}');
        $client = self::client($http);

        $err = self::expectSourcesError(SourcesUnavailableError::class, static fn(): mixed => $client->listSources('acme'));

        self::assertSame(503, $err->status);
        self::assertSame(['error' => 'boom'], $err->body);
    }

    public function testTransportErrorMapsToSourcesUnavailableError(): void
    {
        $client = self::client(RecordingHttpClient::unreachable());

        $err = self::expectSourcesError(SourcesUnavailableError::class, static fn(): mixed => $client->getSource('acme', 'billing'));

        self::assertNull($err->status);
        self::assertNull($err->body);

        $previous = $err->getPrevious();
        self::assertNotNull($previous);
        self::assertSame(ConnectException::class, $previous::class);
    }

    /* --- input validation --------------------------------------------------- */

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function invalidIdentifiers(): iterable
    {
        yield 'parent dir' => ['..'];
        yield 'slash' => ['a/b'];
        yield 'query' => ['a?b=1'];
        yield 'fragment' => ['a#b'];
        yield 'too long' => [str_repeat('a', 65)];
        yield 'empty' => [''];
    }

    #[DataProvider('invalidIdentifiers')]
    public function testEveryMethodRejectsAnInvalidTenantWithoutARequest(string $tenant): void
    {
        foreach (self::tenantCalls($tenant) as $call) {
            $http = RecordingHttpClient::unreachable();
            $client = new SourcesClient(self::BASE_URL, httpClient: $http);

            $err = self::expectSourcesError(SourceInvalidError::class, static function () use ($call, $client): void {
                $call($client);
            });

            self::assertSame('invalid tenant: ' . self::repr($tenant), $err->getMessage());
            self::assertNull($err->status);
            self::assertNull($err->body);
            self::assertSame([], $http->requests());
        }
    }

    #[DataProvider('invalidIdentifiers')]
    public function testSourceMethodsRejectAnInvalidNameWithoutARequest(string $name): void
    {
        foreach (self::nameCalls($name) as $call) {
            $http = RecordingHttpClient::unreachable();
            $client = new SourcesClient(self::BASE_URL, httpClient: $http);

            $err = self::expectSourcesError(SourceInvalidError::class, static function () use ($call, $client): void {
                $call($client);
            });

            self::assertSame('invalid source name: ' . self::repr($name), $err->getMessage());
            self::assertNull($err->status);
            self::assertNull($err->body);
            self::assertSame([], $http->requests());
        }
    }

    public function testValidIdentifiersWithDashAndUnderscoreAreSentAsThePath(): void
    {
        $http = new RecordingHttpClient(static function (RequestInterface $request): ResponseInterface {
            if ($request->getMethod() === 'DELETE') {
                return new Response(204);
            }

            return self::jsonResponse(200, ['tenant' => 'acme-corp', 'entries' => []]);
        });

        $client = self::client($http);
        $client->listSources('acme-corp');
        $client->deleteSource('acme-corp', 'my_source-1');

        self::assertSame('/v1/tenants/acme-corp/sources', self::requestPath($http, 0));
        self::assertSame('/v1/tenants/acme-corp/sources/my_source-1', self::requestPath($http, 1));
    }

    /* --- helpers ------------------------------------------------------------ */

    private static function client(RecordingHttpClient $http, ?string $expectedVersion = null): SourcesClient
    {
        return new SourcesClient(self::BASE_URL, expectedVersion: $expectedVersion, httpClient: $http);
    }

    private static function spec(): SourceSpec
    {
        return new SourceSpec(
            sinks: [['type' => 'log']],
            verify: ['type' => 'hmac', 'secret' => 's3cr3t', 'signature_header' => 'X-Sig'],
            onVerifyFailure: 'reject',
        );
    }

    /**
     * @return list<\Closure>
     */
    private static function tenantCalls(string $tenant): array
    {
        return [
            static fn(SourcesClient $client): array => $client->listSources($tenant),
            static fn(SourcesClient $client): Source => $client->getSource($tenant, 'billing'),
            static fn(SourcesClient $client): Source => $client->createSource($tenant, 'billing', self::spec()),
            static fn(SourcesClient $client): Source => $client->updateSource($tenant, 'billing', self::spec()),
            static function (SourcesClient $client) use ($tenant): void {
                $client->deleteSource($tenant, 'billing');
            },
        ];
    }

    /**
     * @return list<\Closure>
     */
    private static function nameCalls(string $name): array
    {
        return [
            static fn(SourcesClient $client): Source => $client->getSource('acme', $name),
            static fn(SourcesClient $client): Source => $client->createSource('acme', $name, self::spec()),
            static fn(SourcesClient $client): Source => $client->updateSource('acme', $name, self::spec()),
            static function (SourcesClient $client) use ($name): void {
                $client->deleteSource('acme', $name);
            },
        ];
    }

    private static function healthOrSourcesHandler(): RecordingHttpClient
    {
        return new RecordingHttpClient(static function (RequestInterface $request): ResponseInterface {
            if ($request->getUri()->getPath() === '/health') {
                return self::jsonResponse(200, ['status' => 'ok', 'version' => '0.3.0']);
            }

            return self::jsonResponse(200, ['tenant' => 'acme', 'entries' => []]);
        });
    }

    /**
     * @param class-string<SourcesError> $class
     */
    private static function expectSourcesError(string $class, \Closure $call): SourcesError
    {
        try {
            $call();
        } catch (SourcesError $err) {
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

    /**
     * @return array<array-key, mixed>
     */
    private static function decodedBody(RecordingHttpClient $http, int $index): array
    {
        $decoded = json_decode(self::body($http, $index), true, 512, JSON_THROW_ON_ERROR);
        if (!\is_array($decoded)) {
            self::fail('the recorded body is not a JSON object');
        }

        return $decoded;
    }

    private static function healthRequestCount(RecordingHttpClient $http): int
    {
        $count = 0;
        foreach ($http->requests() as $request) {
            if ($request['path'] === '/health') {
                $count++;
            }
        }

        return $count;
    }

    /**
     * The PHP twin of Python's `{value!r}`: the same rendering
     * `HttpTransport::describe()` uses, so a value like `a/b` stays unescaped.
     */
    private static function repr(string $value): string
    {
        return self::encode($value);
    }

    private static function encode(mixed $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function jsonResponse(int $status, array $data): Response
    {
        return new Response($status, ['content-type' => 'application/json'], self::encode($data));
    }
}
