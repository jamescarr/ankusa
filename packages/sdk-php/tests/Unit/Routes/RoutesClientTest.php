<?php

declare(strict_types=1);

namespace Ankusa\Tests\Unit\Routes;

use Ankusa\Routes\InvalidRouteIdError;
use Ankusa\Routes\RouteNotFoundError;
use Ankusa\Routes\RoutesClient;
use Ankusa\Routes\RoutesError;
use Ankusa\Routes\RoutesRejectedError;
use Ankusa\Routes\RoutesUnavailableError;
use Ankusa\Tests\Support\RecordingHttpClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * The routes vectors cover the request bodies and status classification; this
 * file covers the wire shapes they cannot express: the nested empty JSON object
 * rule, dropped `null` query parameters, the id escaping/refusal rules, and the
 * fields a rejected request carries.
 */
final class RoutesClientTest extends TestCase
{
    private const string BASE_URL = 'http://routes.test';

    public function testCreateRouteSendsNestedEmptyObjectsAndListsVerbatim(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"id":"stripe"}');
        $client = self::client($http);

        self::assertSame(['id' => 'stripe'], $client->createRoute(['metadata' => new \stdClass(), 'tags' => []]));

        self::assertSame('{"metadata":{},"tags":[]}', self::body($http, 0));
        self::assertSame('/admin/routes', self::requestPath($http, 0));
    }

    public function testCallerContentTypeIsReplacedNotDuplicatedOnJsonBodies(): void
    {
        $lines = [];
        $http = new RecordingHttpClient(static function (RequestInterface $request) use (&$lines): ResponseInterface {
            $lines[] = $request->getHeaderLine('content-type');

            return new Response(201, [], '{"id":"stripe"}');
        });
        $client = new RoutesClient(self::BASE_URL, ['Content-Type' => 'text/plain', 'X-Token' => 't'], 10.0, $http);

        $client->createRoute(['path' => '/a']);

        self::assertSame(['application/json'], $lines);
        self::assertSame('t', self::requestAt($http, 0)['headers']['x-token'] ?? null);
    }

    public function testListRoutesDropsNullsAndSendsBooleansAsTrueFalse(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"routes":[],"next_cursor":null}');
        $client = self::client($http);

        $client->listRoutes(['enabled' => true, 'limit' => 10, 'cursor' => null]);
        $client->listRoutes(['enabled' => false]);

        $request = self::requestAt($http, 0);
        self::assertSame('GET', $request['method']);
        self::assertSame('/admin/routes?enabled=true&limit=10', $request['path']);
        self::assertSame('/admin/routes?enabled=false', self::requestPath($http, 1));
    }

    public function testRouteIdsArePercentEncodedAsOnePathSegment(): void
    {
        $http = RecordingHttpClient::canned(200, [], '{"id":"x"}');
        $client = self::client($http);

        $client->getRoute('a/b c');
        $client->getRoute('a/b?c#d e%');

        self::assertSame('/admin/routes/a%2Fb%20c', self::requestPath($http, 0));
        self::assertSame('/admin/routes/a%2Fb%3Fc%23d%20e%25', self::requestPath($http, 1));
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function unusableRouteIds(): iterable
    {
        yield 'empty' => [''];
        yield 'dot' => ['.'];
        yield 'dot dot' => ['..'];
    }

    #[DataProvider('unusableRouteIds')]
    public function testUnusableRouteIdsAreRefusedBeforeAnyRequest(string $id): void
    {
        foreach (self::idCalls($id) as $call) {
            $http = RecordingHttpClient::unreachable();
            $client = self::client($http);

            $err = self::expectRoutesError(InvalidRouteIdError::class, static function () use ($call, $client): void {
                $call($client);
            });

            self::assertSame('invalid route id ' . self::repr($id), $err->getMessage());
            self::assertFalse($err->isRetryable());
            self::assertSame([], $http->requests());
        }
    }

    public function testDeleteRouteIsVoidAndIssuesDelete(): void
    {
        $http = RecordingHttpClient::canned(204);

        self::client($http)->deleteRoute('stripe');

        $request = self::requestAt($http, 0);
        self::assertSame('DELETE', $request['method']);
        self::assertSame('/admin/routes/stripe', $request['path']);
        self::assertNull($request['body']);
    }

    public function testNotFoundIsRouteNotFoundWhateverTheBody(): void
    {
        foreach (['{}', '{"error":"not_found"}'] as $payload) {
            $http = RecordingHttpClient::canned(404, [], $payload);
            $client = self::client($http);

            $err = self::expectRoutesError(RouteNotFoundError::class, static fn(): mixed => $client->getRoute('nope'));

            self::assertSame('route not found (404)', $err->getMessage());
            self::assertFalse($err->isRetryable());
        }
    }

    public function testRejectedRequestCarriesEveryBodyField(): void
    {
        $http = RecordingHttpClient::canned(
            400,
            [],
            '{"error":"invalid_route","field":"path","message":"must start with \"/\"","conflicting_id":"stripe","max_routes":50}',
        );
        $client = self::client($http);

        try {
            $client->createRoute(['path' => 'hooks/x']);
            self::fail('expected RoutesRejectedError');
        } catch (RoutesRejectedError $err) {
            self::assertSame(400, $err->status);
            self::assertSame('invalid_route', $err->errorCode);
            self::assertSame('path', $err->field);
            self::assertSame('must start with "/"', $err->detail);
            self::assertSame('stripe', $err->conflictingId);
            self::assertSame(50, $err->maxRoutes);
            self::assertFalse($err->isRetryable());
        }
    }

    public function testRejectedRequestWithWrongFieldTypesCarriesNulls(): void
    {
        $http = RecordingHttpClient::canned(
            403,
            [],
            '{"error":7,"field":[],"message":null,"conflicting_id":1.5,"max_routes":"many"}',
        );
        $client = self::client($http);

        try {
            $client->listRoutes();
            self::fail('expected RoutesRejectedError');
        } catch (RoutesRejectedError $err) {
            self::assertSame(403, $err->status);
            self::assertNull($err->errorCode);
            self::assertNull($err->field);
            self::assertNull($err->detail);
            self::assertNull($err->conflictingId);
            self::assertNull($err->maxRoutes);
            self::assertStringContainsString('routes listener rejected the request (403)', $err->getMessage());
        }
    }

    public function testServerErrorsAndRedirectsAreUnavailable(): void
    {
        foreach ([500, 503, 302] as $status) {
            $http = RecordingHttpClient::canned($status, [], '{"error":"boom"}');
            $client = self::client($http);

            $err = self::expectRoutesError(RoutesUnavailableError::class, static fn(): mixed => $client->listRoutes());

            self::assertTrue($err->isRetryable());
            self::assertStringContainsString("routes listener error ({$status})", $err->getMessage());
        }
    }

    public function testTransportFailureIsUnavailableWithThePreviousError(): void
    {
        $client = self::client(RecordingHttpClient::unreachable());

        $err = self::expectRoutesError(RoutesUnavailableError::class, static fn(): mixed => $client->health());

        self::assertTrue($err->isRetryable());

        $previous = $err->getPrevious();
        self::assertNotNull($previous);
        self::assertSame(ConnectException::class, $previous::class);
    }

    /* --- helpers ------------------------------------------------------------ */

    private static function client(RecordingHttpClient $http): RoutesClient
    {
        return new RoutesClient(self::BASE_URL, httpClient: $http);
    }

    /**
     * @return list<\Closure>
     */
    private static function idCalls(string $id): array
    {
        return [
            static fn(RoutesClient $client): array => $client->getRoute($id),
            static fn(RoutesClient $client): array => $client->replaceRoute($id, ['path' => '/webhooks/stripe']),
            static fn(RoutesClient $client): array => $client->updateRoute($id, ['enabled' => true]),
            static function (RoutesClient $client) use ($id): void {
                $client->deleteRoute($id);
            },
        ];
    }

    /**
     * @param class-string<RoutesError> $class
     */
    private static function expectRoutesError(string $class, \Closure $call): RoutesError
    {
        try {
            $call();
        } catch (RoutesError $err) {
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

    private static function repr(string $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
