<?php

declare(strict_types=1);

/**
 * The conformance runner's mock gateway: a router script for PHP's built-in
 * server, the PHP counterpart of the Python runner's `ThreadingHTTPServer`.
 *
 * It answers every request with the response `$ANKUSA_GATEWAY_DIR/spec.json`
 * describes and appends one JSON line per request to
 * `$ANKUSA_GATEWAY_DIR/requests.jsonl` — before sleeping, so a client that
 * gives up on a delayed response never races the record it belongs to.
 *
 * The env var is set by ConformanceTest::setUpBeforeClass().
 */

require __DIR__ . '/../../vendor/autoload.php';

/**
 * A JSON object as pairs, decoded object-mode or assoc-mode.
 *
 * @return array<string, mixed>
 */
function object(mixed $value): array
{
    if ($value === null) {
        return [];
    }
    if ($value instanceof stdClass) {
        $value = get_object_vars($value);
    }
    if (!\is_array($value)) {
        throw new RuntimeException('expected a JSON object, got ' . get_debug_type($value));
    }

    $object = [];
    foreach ($value as $key => $item) {
        if (!\is_string($key)) {
            throw new RuntimeException('expected a JSON object, got an array with integer keys');
        }
        $object[$key] = $item;
    }

    return $object;
}

function text(mixed $value, string $what): string
{
    if (!\is_string($value)) {
        throw new RuntimeException("{$what} is not a string");
    }

    return $value;
}

function number(mixed $value, string $what): int
{
    if (!\is_int($value)) {
        throw new RuntimeException("{$what} is not an integer");
    }

    return $value;
}

/**
 * @return array<string, string>
 */
function headerMap(mixed $headers): array
{
    if (!\is_array($headers)) {
        return [];
    }

    $map = [];
    foreach ($headers as $name => $value) {
        if (\is_string($name) && \is_string($value)) {
            $map[strtolower($name)] = $value;
        }
    }

    return $map;
}

$dir = getenv('ANKUSA_GATEWAY_DIR');
if (!\is_string($dir) || $dir === '') {
    http_response_code(500);
    echo 'ANKUSA_GATEWAY_DIR is not set';

    return;
}

$spec = object(json_decode((string) file_get_contents($dir . '/spec.json'), false, 512, JSON_THROW_ON_ERROR));

file_put_contents(
    $dir . '/requests.jsonl',
    json_encode(
        [
            'method' => $_SERVER['REQUEST_METHOD'],
            'path' => $_SERVER['REQUEST_URI'],
            'headers' => headerMap(getallheaders()),
            'body' => ($body = (string) file_get_contents('php://input')) === '' ? null : $body,
        ],
        JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
    ) . "\n",
    FILE_APPEND,
);

$delay = \is_int($spec['delay_ms'] ?? null) ? $spec['delay_ms'] : 0;
if ($delay > 0) {
    usleep($delay * 1000);
}

$payload = '';
$body = object($spec['body'] ?? null);
if (\array_key_exists('text', $body)) {
    $payload = text($body['text'], 'Body.text');
} elseif (\array_key_exists('base64', $body)) {
    $decoded = base64_decode(text($body['base64'], 'Body.base64'), true);
    if ($decoded === false) {
        throw new RuntimeException('Body.base64 is not valid base64');
    }
    $payload = $decoded;
} elseif (\array_key_exists('json', $body)) {
    $payload = json_encode($body['json'], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
}

ini_set('default_mimetype', '');
header_remove('X-Powered-By');

foreach (object($spec['headers'] ?? null) as $name => $value) {
    header($name . ': ' . text($value, "header {$name}"));
}
header('Content-Length: ' . \strlen($payload));
http_response_code(number($spec['status'] ?? null, 'status'));
echo $payload;
