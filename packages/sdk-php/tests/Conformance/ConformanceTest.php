<?php

declare(strict_types=1);

namespace Ankusa\Tests\Conformance;

use Ankusa\Admin\AdminClient;
use Ankusa\Admin\AdminError;
use Ankusa\Admin\AdminRejectedError;
use Ankusa\Admin\AdminUnavailableError;
use Ankusa\Admin\RoleNotEnabledError;
use Ankusa\ClaimCheck\ClaimCheckClient;
use Ankusa\ClaimCheck\ClaimCheckUnavailableError;
use Ankusa\ClaimCheck\ClaimIntegrityError;
use Ankusa\ClaimCheck\ClaimNotFoundError;
use Ankusa\ClaimCheck\ClaimRejectedError;
use Ankusa\ClaimCheck\InvalidClaimRefError;
use Ankusa\ClaimCheck\ParsedClaimRef;
use Ankusa\Message\InvalidMessageError;
use Ankusa\Message\Message;
use Ankusa\Routes\InvalidRouteIdError;
use Ankusa\Routes\RouteNotFoundError;
use Ankusa\Routes\RoutesClient;
use Ankusa\Routes\RoutesError;
use Ankusa\Routes\RoutesRejectedError;
use Ankusa\Routes\RoutesUnavailableError;
use Ankusa\Tests\Support\RecordingHttpClient;
use Ankusa\Webhook\HookHeaders;
use Ankusa\Webhook\MissingHookIdError;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Run the language-neutral conformance vectors in `conformance/` against this
 * SDK.
 *
 * One test per vector, named by its `id`. See `conformance/README.md` for the
 * vector format and the runner contract every SDK follows: every `cases/*.json`
 * case is registered, nothing is filtered or skipped, and an unknown
 * `operation` fails.
 *
 * A vector's `gateway` is either a real HTTP server (PHP's built-in server
 * running `gateway.php`, which records every request it sees), a `"transport":
 * "injected"` PSR-18 client that answers in-process, or `"unreachable": true`.
 */
final class ConformanceTest extends TestCase
{
    /** The exact exported class name a vector's `expect.error.class` names. */
    private const array ERROR_CLASSES = [
        'InvalidClaimRefError' => InvalidClaimRefError::class,
        'ClaimNotFoundError' => ClaimNotFoundError::class,
        'ClaimRejectedError' => ClaimRejectedError::class,
        'ClaimIntegrityError' => ClaimIntegrityError::class,
        'ClaimCheckUnavailableError' => ClaimCheckUnavailableError::class,
        'MissingHookIdError' => MissingHookIdError::class,
        'InvalidMessageError' => InvalidMessageError::class,
        'RoutesError' => RoutesError::class,
        'InvalidRouteIdError' => InvalidRouteIdError::class,
        'RoutesUnavailableError' => RoutesUnavailableError::class,
        'RouteNotFoundError' => RouteNotFoundError::class,
        'RoutesRejectedError' => RoutesRejectedError::class,
        'AdminError' => AdminError::class,
        'AdminUnavailableError' => AdminUnavailableError::class,
        'RoleNotEnabledError' => RoleNotEnabledError::class,
        'AdminRejectedError' => AdminRejectedError::class,
    ];

    /**
     * `expect.error` key => the property or method holding it. PHP's own
     * `message`/`code` are taken by {@see \Throwable}, so the server's
     * `message` field is `detail` and its `error` field is `errorCode`.
     */
    private const array ERROR_ATTRIBUTES = [
        'retryable' => 'isRetryable',
        'status' => 'status',
        'body' => 'body',
        'code' => 'errorCode',
        'field' => 'field',
        'message' => 'detail',
        'conflicting_id' => 'conflictingId',
        'max_routes' => 'maxRoutes',
        'role' => 'role',
    ];

    private static string $dir = '';

    private static mixed $server = null;

    private static int $port = 0;

    public static function setUpBeforeClass(): void
    {
        self::assertNotEmpty(self::caseFiles(), 'conformance/cases/*.json is empty');

        self::$dir = sys_get_temp_dir() . '/ankusa-php-conformance-' . bin2hex(random_bytes(4));
        if (!mkdir(self::$dir, 0o777, true) && !is_dir(self::$dir)) {
            self::fail('could not create ' . self::$dir);
        }
        self::$port = self::freePort();

        $process = proc_open(
            [PHP_BINARY, '-S', '127.0.0.1:' . self::$port, __DIR__ . '/gateway.php'],
            [
                0 => ['file', '/dev/null', 'r'],
                1 => ['file', self::$dir . '/server.log', 'a'],
                2 => ['file', self::$dir . '/server.log', 'a'],
            ],
            $pipes,
            null,
            getenv() + ['ANKUSA_GATEWAY_DIR' => self::$dir],
        );
        if (!\is_resource($process)) {
            self::fail('could not start the conformance gateway');
        }
        self::$server = $process;

        $deadline = microtime(true) + 5.0;
        while (microtime(true) < $deadline) {
            $socket = @fsockopen('127.0.0.1', self::$port, $errno, $errstr, 0.1);
            if (\is_resource($socket)) {
                fclose($socket);

                return;
            }
            usleep(50_000);
        }

        self::fail('the conformance gateway did not start on port ' . self::$port . ': ' . self::serverLog());
    }

    public static function tearDownAfterClass(): void
    {
        if (\is_resource(self::$server)) {
            proc_terminate(self::$server);
            proc_close(self::$server);
            self::$server = null;
        }

        if (self::$dir !== '' && is_dir(self::$dir)) {
            foreach ((array) glob(self::$dir . '/*') as $file) {
                if (\is_string($file)) {
                    unlink($file);
                }
            }
            rmdir(self::$dir);
            self::$dir = '';
        }
    }

    /**
     * @return iterable<string, array{0: \stdClass}>
     */
    public static function cases(): iterable
    {
        foreach (self::caseFiles() as $file) {
            $document = json_decode((string) file_get_contents($file), false, 512, JSON_THROW_ON_ERROR);
            $cases = $document instanceof \stdClass ? ($document->cases ?? null) : null;
            if (!\is_array($cases)) {
                self::fail($file . ' is not a {"cases": [...]} document');
            }

            foreach ($cases as $case) {
                if (!$case instanceof \stdClass) {
                    self::fail($file . ' has a case that is not an object');
                }
                $id = $case->id ?? null;
                if (!\is_string($id)) {
                    self::fail($file . ' has a case without an id');
                }

                yield $id => [$case];
            }
        }
    }

    #[DataProvider('cases')]
    public function testConformance(\stdClass $case): void
    {
        $operation = \is_string($case->operation ?? null) ? $case->operation : '';

        $inp = self::object(self::input($case->input ?? null), 'input');
        $conn = self::connection($inp);

        $ok = null;
        $error = null;
        try {
            $ok = $this->dispatch($operation, $inp, $conn);
        } catch (\Throwable $err) {
            $class = self::errorClass($err);
            if ($class === null) {
                throw $err;
            }
            $error = ['class' => $class] + self::errorAttributes($err);
        }

        $expect = $case->expect ?? null;
        if (!$expect instanceof \stdClass) {
            self::fail('the case has no expect object');
        }

        if (property_exists($expect, 'ok')) {
            self::assertNull($error, 'expected a value, got ' . self::show($error));
            self::assertSameJson(self::expectedOk($operation, $expect->ok), $ok, 'ok value');
        } elseif (property_exists($expect, 'error')) {
            $want = $expect->error;
            if (!$want instanceof \stdClass) {
                self::fail('expect.error is not an object');
            }
            if ($error === null) {
                self::fail('expected ' . self::show($want) . ', got ' . self::show($ok));
            }
            self::assertError($want, $error);
        }

        if (property_exists($expect, 'requests')) {
            self::assertRequests(
                $conn['http']?->requests() ?? self::recordedRequests(),
                self::list($expect->requests, 'expect.requests'),
            );
        }
    }

    /**
     * @param array<string, mixed> $inp
     * @param array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float} $conn
     */
    private function dispatch(string $operation, array $inp, array $conn): mixed
    {
        return match ($operation) {
            'parse_claim_ref' => self::parsedClaimRef($inp),
            'parse_headers' => self::hookHeaders($inp),
            'redeem' => self::redeem($inp, $conn),
            'health' => self::claimCheck($conn)->health(),
            'routes_health' => self::routes($conn)->health(),
            'routes_list' => self::routes($conn)->listRoutes(self::scalars($inp, 'params')),
            'routes_create' => self::routes($conn)->createRoute(self::object($inp['input'] ?? null, 'input')),
            'routes_get' => self::routes($conn)->getRoute(self::string($inp, 'id')),
            'routes_replace' => self::routes($conn)->replaceRoute(self::string($inp, 'id'), self::object($inp['input'] ?? null, 'input')),
            'routes_update' => self::routes($conn)->updateRoute(self::string($inp, 'id'), self::object($inp['patch'] ?? null, 'patch')),
            'routes_delete' => self::routes($conn)->deleteRoute(self::string($inp, 'id')),
            'routes_ip_rules_get' => self::routes($conn)->getIpRules(),
            'routes_ip_rules_put' => self::routes($conn)->putIpRules(self::object($inp['rules'] ?? null, 'rules')),
            'routes_test' => self::routes($conn)->testRoute(self::object($inp['request'] ?? null, 'request')),
            'admin_health' => self::admin($conn)->health(),
            'admin_metrics' => ['text' => self::admin($conn)->metrics()],
            'admin_config' => self::admin($conn)->config(),
            'admin_dlq_list' => self::admin($conn)->listDeadLetters(self::scalars($inp, 'params')),
            'admin_quarantine' => self::admin($conn)->listQuarantined(self::scalars($inp, 'params')),
            'admin_replay_create' => self::admin($conn)->createReplay(self::object($inp['spec'] ?? null, 'spec')),
            'admin_replay_get' => self::admin($conn)->getReplay(self::string($inp, 'id')),
            'admin_replay_list' => self::admin($conn)->listReplays(),
            'admin_replay_update' => self::admin($conn)->updateReplay(self::string($inp, 'id'), self::object($inp['patch'] ?? null, 'patch')),
            'decode_message' => self::decodeMessage($inp),
            'idempotency_key' => self::idempotencyKey($inp),
            default => self::fail("unknown conformance operation {$operation}"),
        };
    }

    /*
     * The three ways a vector connects the SDK to its `gateway`.
     */

    /**
     * @param array<string, mixed> $inp
     *
     * @return array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float}
     */
    private static function connection(array $inp): array
    {
        $client = self::object($inp['client'] ?? null, 'client');
        $gateway = self::object($inp['gateway'] ?? null, 'gateway');
        $headers = self::stringMap($client['headers'] ?? null);
        $timeoutMs = $client['timeout_ms'] ?? 10_000;
        $timeout = \is_int($timeoutMs) || \is_float($timeoutMs) ? $timeoutMs / 1000 : 10.0;

        // Every case starts from an empty record, whichever transport it uses:
        // a case that must make no request at all asserts on that record.
        file_put_contents(self::$dir . '/requests.jsonl', '');

        if (($client['transport'] ?? null) === 'injected') {
            return [
                'baseUrl' => 'http://gateway.invalid',
                'http' => RecordingHttpClient::canned(
                    self::int($gateway, 'status'),
                    self::stringMap($gateway['headers'] ?? null),
                    self::bodyBytes($gateway['body'] ?? null),
                ),
                'headers' => $headers,
                'timeout' => $timeout,
            ];
        }

        if (($gateway['unreachable'] ?? false) === true) {
            return ['baseUrl' => 'http://127.0.0.1:1', 'http' => null, 'headers' => $headers, 'timeout' => $timeout];
        }

        // A real server: hand it the response to serve.
        file_put_contents(
            self::$dir . '/spec.json',
            json_encode((object) $gateway, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        );

        return ['baseUrl' => 'http://127.0.0.1:' . self::$port, 'http' => null, 'headers' => $headers, 'timeout' => $timeout];
    }

    /**
     * @param array<string, mixed>                                            $inp
     * @param array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float} $conn
     *
     * @return array{body: array{base64: string}}
     */
    private static function redeem(array $inp, array $conn): array
    {
        $bytes = self::claimCheck($conn)->redeem(self::string($inp, 'ref'), self::string($inp, 'sha256'));

        return ['body' => ['base64' => base64_encode($bytes)]];
    }

    /**
     * @param array<string, mixed> $inp
     *
     * @return array{tenant_id: string, claim_id: string, path: string}
     */
    private static function parsedClaimRef(array $inp): array
    {
        $parsed = ParsedClaimRef::parse(self::string($inp, 'ref'));

        return ['tenant_id' => $parsed->tenantId, 'claim_id' => $parsed->claimId, 'path' => $parsed->path];
    }

    /**
     * @param array<string, mixed> $inp
     *
     * @return array{id: string, source: string, tenant: string|null, content_type: string|null, dedupe_key: string|null, replay_id: string|null}
     */
    private static function hookHeaders(array $inp): array
    {
        $headers = HookHeaders::fromHeaders(self::stringMap($inp['headers'] ?? null));

        return [
            'id' => $headers->id,
            'source' => $headers->source,
            'tenant' => $headers->tenant,
            'content_type' => $headers->contentType,
            'dedupe_key' => $headers->dedupeKey,
            'replay_id' => $headers->replayId,
        ];
    }

    /**
     * @param array<string, mixed> $inp
     *
     * @return array<string, mixed>
     */
    private static function decodeMessage(array $inp): array
    {
        $message = Message::decode(self::string($inp, 'message'));
        $body = $message->body;

        return [
            'v' => $message->v,
            'id' => $message->id,
            'source_id' => $message->sourceId,
            'tenant_id' => $message->tenantId,
            'received_at' => $message->receivedAt,
            'content_type' => $message->contentType,
            'size' => $message->size,
            'body_base64' => $body === null ? null : base64_encode($body),
            'claim' => $message->claim,
            'sha256' => $message->sha256,
            'dedupe_key' => $message->dedupeKey,
            'replay_id' => $message->replayId,
            'headers' => $message->headers,
        ];
    }

    /**
     * @param array<string, mixed> $inp
     *
     * @return array{key: string}
     */
    private static function idempotencyKey(array $inp): array
    {
        $includeReplay = ($inp['include_replay'] ?? null) === true;
        $hook = \array_key_exists('headers', $inp)
            ? HookHeaders::fromHeaders(self::stringMap($inp['headers'] ?? null))
            : Message::decode(self::string($inp, 'message'));

        return ['key' => $hook->idempotencyKey($includeReplay)];
    }

    /**
     * @param array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float} $conn
     */
    private static function claimCheck(array $conn): ClaimCheckClient
    {
        return new ClaimCheckClient($conn['baseUrl'], $conn['headers'], $conn['timeout'], $conn['http']);
    }

    /**
     * @param array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float} $conn
     */
    private static function routes(array $conn): RoutesClient
    {
        return new RoutesClient($conn['baseUrl'], $conn['headers'], $conn['timeout'], $conn['http']);
    }

    /**
     * @param array{baseUrl: string, http: ?RecordingHttpClient, headers: array<string, string>, timeout: float} $conn
     */
    private static function admin(array $conn): AdminClient
    {
        return new AdminClient($conn['baseUrl'], $conn['headers'], $conn['timeout'], $conn['http']);
    }

    /*
     * Assertions.
     */

    /**
     * Bytes can't round-trip through JSON: both the expected `Body` and the
     * returned bytes become base64.
     */
    private static function expectedOk(string $operation, mixed $ok): mixed
    {
        if ($operation !== 'redeem') {
            return $ok;
        }

        $body = $ok instanceof \stdClass ? ($ok->body ?? null) : null;

        return ['body' => ['base64' => base64_encode(self::bodyBytes($body))]];
    }

    /**
     * @param array<string, mixed> $error
     */
    private static function assertError(\stdClass $want, array $error): void
    {
        $expectedClass = $want->class ?? null;
        if (!\is_string($expectedClass) || !isset(self::ERROR_CLASSES[$expectedClass])) {
            self::fail('expect.error.class is not an exported SDK error: ' . self::show($expectedClass));
        }
        self::assertSame($expectedClass, $error['class'] ?? null, 'error class');

        foreach (get_object_vars($want) as $key => $value) {
            if ($key === 'class') {
                continue;
            }
            if (!\array_key_exists($key, self::ERROR_ATTRIBUTES)) {
                self::fail("expect.error has an unknown key {$key}");
            }
            if (!\array_key_exists($key, $error)) {
                self::fail(sprintf('%s has no attribute %s (expected %s)', $expectedClass, self::ERROR_ATTRIBUTES[$key], self::show($value)));
            }
            self::assertSameJson($value, $error[$key], "{$expectedClass}.{$key}");
        }
    }

    /**
     * The attributes a vector can assert, read off the thrown error; absent
     * ones are simply not compared.
     *
     * @return array<string, mixed>
     */
    private static function errorAttributes(\Throwable $err): array
    {
        $reflection = new \ReflectionObject($err);
        $attributes = [];

        foreach (self::ERROR_ATTRIBUTES as $key => $accessor) {
            if ($reflection->hasProperty($accessor)) {
                $attributes[$key] = $reflection->getProperty($accessor)->getValue($err);
            } elseif ($reflection->hasMethod($accessor)) {
                $attributes[$key] = $reflection->getMethod($accessor)->invoke($err);
            }
        }

        return $attributes;
    }

    private static function errorClass(\Throwable $err): ?string
    {
        foreach (self::ERROR_CLASSES as $name => $class) {
            if ($err::class === $class) {
                return $name;
            }
        }

        return null;
    }

    /**
     * @param list<array{method: string, path: string, headers: array<string, string>, body: string|null}> $actual
     * @param array<array-key, mixed>                                                                       $expected
     */
    private static function assertRequests(array $actual, array $expected): void
    {
        self::assertCount(\count($expected), $actual, 'recorded requests: ' . self::show($actual));

        foreach (array_values($expected) as $i => $want) {
            $got = $actual[$i];
            $want = self::object($want, "expect.requests[{$i}]");

            self::assertSame(self::string($want, 'method'), $got['method'], "requests[{$i}].method");
            self::assertSame(self::string($want, 'path'), $got['path'], "requests[{$i}].path");

            foreach (self::stringMap($want['headers'] ?? null) as $name => $value) {
                self::assertSame($value, $got['headers'][$name] ?? null, "requests[{$i}].headers.{$name}");
            }

            if (\array_key_exists('body', $want)) {
                $recorded = $got['body'] === null
                    ? null
                    : json_decode($got['body'], false, 512, JSON_THROW_ON_ERROR);
                self::assertSameJson($want['body'], $recorded, "requests[{$i}].body");
            }
        }
    }

    /**
     * @return list<array{method: string, path: string, headers: array<string, string>, body: string|null}>
     */
    private static function recordedRequests(): array
    {
        $path = self::$dir . '/requests.jsonl';
        $raw = is_file($path) ? (string) file_get_contents($path) : '';

        $requests = [];
        foreach (explode("\n", $raw) as $line) {
            if (trim($line) === '') {
                continue;
            }
            $recorded = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            if (!\is_array($recorded)) {
                self::fail('the gateway recorded a non-object: ' . $line);
            }

            $body = $recorded['body'] ?? null;
            $requests[] = [
                'method' => \is_string($recorded['method'] ?? null) ? $recorded['method'] : '',
                'path' => \is_string($recorded['path'] ?? null) ? $recorded['path'] : '',
                'headers' => self::stringMap($recorded['headers'] ?? null),
                'body' => \is_string($body) ? $body : null,
            ];
        }

        return $requests;
    }

    /*
     * Reading the vectors.
     */

    /**
     * A vector's `input`, with JSON objects as arrays (recursively) so they can
     * be handed to the SDK as request bodies. An empty object stays a
     * `stdClass`, so a nested `{}` still goes out as `{}` rather than `[]`.
     */
    private static function input(mixed $value): mixed
    {
        if ($value instanceof \stdClass) {
            $vars = get_object_vars($value);
            if ($vars === []) {
                return new \stdClass();
            }

            return array_map(self::input(...), $vars);
        }

        if (\is_array($value)) {
            return array_map(self::input(...), $value);
        }

        return $value;
    }

    /**
     * The bytes a vector's `Body` describes: `text` (UTF-8) verbatim, `base64`
     * decoded, `json` as compact JSON, nothing at all as zero bytes.
     */
    private static function bodyBytes(mixed $body): string
    {
        $spec = self::assoc($body);
        if ($spec === null) {
            return '';
        }
        if (!\is_array($spec)) {
            self::fail('a Body is not an object: ' . self::show($body));
        }

        if (\array_key_exists('text', $spec)) {
            if (!\is_string($spec['text'])) {
                self::fail('Body.text is not a string: ' . self::show($body));
            }

            return $spec['text'];
        }

        if (\array_key_exists('base64', $spec)) {
            if (!\is_string($spec['base64'])) {
                self::fail('Body.base64 is not a string: ' . self::show($body));
            }
            $decoded = base64_decode($spec['base64'], true);
            if ($decoded === false) {
                self::fail('Body.base64 is not valid base64: ' . $spec['base64']);
            }

            return $decoded;
        }

        if (\array_key_exists('json', $spec)) {
            return json_encode($spec['json'], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }

        self::fail('unknown Body: ' . self::show($body));
    }

    /**
     * Deep equality the way the vectors mean it: both sides go through JSON
     * into plain arrays, and key order isn't significant. (`{}` and `[]` both
     * become `[]`, so no vector can tell them apart; the unit tests pin the
     * `{}` request bodies the SDK must send.)
     */
    private static function assertSameJson(mixed $expected, mixed $actual, string $context): void
    {
        self::assertSame(self::canonical(self::assoc($expected)), self::canonical(self::assoc($actual)), $context);
    }

    /**
     * A JSON value as PHP arrays (`null` for JSON null).
     */
    private static function assoc(mixed $value): mixed
    {
        return json_decode(
            json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
    }

    /**
     * Key order isn't a difference: sort every JSON object's keys, recursively.
     */
    private static function canonical(mixed $value): mixed
    {
        if (!\is_array($value)) {
            return $value;
        }

        $canonical = array_map(self::canonical(...), $value);
        if (!array_is_list($value)) {
            ksort($canonical);
        }

        return $canonical;
    }

    /*
     * Small typed readers for the shapes above; each fails the test rather than
     * letting a malformed vector become a PHP error.
     */

    /**
     * @return array<string, mixed>
     */
    private static function object(mixed $value, string $what): array
    {
        $object = [];
        foreach (self::entries($value, $what) as $key => $item) {
            if (!\is_string($key)) {
                self::fail("{$what} is not an object: " . self::show($value));
            }
            $object[$key] = $item;
        }

        return $object;
    }

    /**
     * A JSON object as pairs: an `stdClass` from the decoded vector, an array
     * from {@see self::input()}, or nothing at all.
     *
     * @return array<array-key, mixed>
     */
    private static function entries(mixed $value, string $what): array
    {
        if ($value === null) {
            return [];
        }
        if ($value instanceof \stdClass) {
            return get_object_vars($value);
        }
        if (\is_array($value)) {
            return $value;
        }

        self::fail("{$what} is not an object: " . self::show($value));
    }

    /**
     * @return array<string, string>
     */
    private static function stringMap(mixed $value): array
    {
        $map = [];
        foreach (self::object($value, 'a header map') as $key => $item) {
            if (!\is_string($item)) {
                self::fail('a header map has a non-string value: ' . self::show($value));
            }
            $map[$key] = $item;
        }

        return $map;
    }

    /**
     * @param array<string, mixed> $inp
     *
     * @return array<string, scalar|null>
     */
    private static function scalars(array $inp, string $key): array
    {
        $map = [];
        foreach (self::object($inp[$key] ?? null, "input.{$key}") as $name => $item) {
            if (!\is_scalar($item) && $item !== null) {
                self::fail("input.{$key} has a non-scalar value: " . self::show($item));
            }
            $map[$name] = $item;
        }

        return $map;
    }

    /**
     * @param array<string, mixed> $inp
     */
    private static function string(array $inp, string $key): string
    {
        $value = $inp[$key] ?? null;
        if (!\is_string($value)) {
            self::fail("input.{$key} is not a string: " . self::show($value));
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $inp
     */
    private static function int(array $inp, string $key): int
    {
        $value = $inp[$key] ?? null;
        if (!\is_int($value)) {
            self::fail("input.{$key} is not an integer: " . self::show($value));
        }

        return $value;
    }

    /**
     * @return list<mixed>
     */
    private static function list(mixed $value, string $what): array
    {
        if (!\is_array($value) || !array_is_list($value)) {
            self::fail("{$what} is not an array: " . self::show($value));
        }

        return $value;
    }

    private static function show(mixed $value): string
    {
        $json = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

        return $json === false ? \get_debug_type($value) : $json;
    }

    /**
     * @return list<string>
     */
    private static function caseFiles(): array
    {
        $files = glob(dirname(__DIR__, 4) . '/conformance/cases/*.json');
        if ($files === false) {
            self::fail('conformance/cases/*.json is unreadable');
        }
        sort($files, SORT_STRING);

        return $files;
    }

    private static function freePort(): int
    {
        $socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if (!\is_resource($socket)) {
            self::fail("could not allocate a port: {$errstr}");
        }
        $name = stream_socket_get_name($socket, false);
        fclose($socket);
        if (!\is_string($name)) {
            self::fail('could not read the allocated port');
        }

        return (int) substr($name, (int) strrpos($name, ':') + 1);
    }

    private static function serverLog(): string
    {
        $log = self::$dir . '/server.log';

        return is_file($log) ? (string) file_get_contents($log) : '(no log)';
    }
}
