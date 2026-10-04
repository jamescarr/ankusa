<?php

declare(strict_types=1);

namespace Ankusa\Tests\Unit\Webhook;

use Ankusa\Webhook\HookHeaders;
use Ankusa\Webhook\MissingHookIdError;
use GuzzleHttp\Psr7\ServerRequest;
use PHPUnit\Framework\TestCase;

/**
 * The webhook vectors cover the plain string map a JSON document can express;
 * this file covers the PHP-only input shapes instead: PSR-7 messages, list- or
 * `null`-valued arrays (`getallheaders()`, `getHeaders()`, `HeaderBag::all()`),
 * and the mixed-case keys those produce.
 */
final class HookHeadersTest extends TestCase
{
    public function testReadsAPsr7RequestWithMixedCaseAndRepeatedHeaders(): void
    {
        $request = new ServerRequest('POST', 'https://hooks.test/inbound', [
            'X-Ankusa-Id' => ['01a0', '01a1'],
            'X-ANKUSA-SOURCE' => 'stripe',
            'x-ankusa-tenant' => 'acme',
            'CONTENT-TYPE' => 'application/json',
        ]);

        $headers = HookHeaders::fromHeaders($request);

        // `getHeader()` is case-insensitive and returns the values in order.
        self::assertSame('01a0', $headers->id);
        self::assertSame('stripe', $headers->source);
        self::assertSame('acme', $headers->tenant);
        self::assertSame('application/json', $headers->contentType);
    }

    public function testAPsr7MessageAndAnArrayParseTheSameDelivery(): void
    {
        $array = [
            'x-ankusa-id' => '01a0',
            'x-ankusa-source' => 'stripe',
            'x-ankusa-tenant' => 'acme',
            'x-ankusa-idempotency-key' => 'acme:stripe:evt_1',
            'content-type' => 'application/json',
        ];

        self::assertSame(
            self::flatten(HookHeaders::fromHeaders($array)),
            self::flatten(HookHeaders::fromHeaders(new ServerRequest('POST', 'https://hooks.test/inbound', $array))),
        );
    }

    public function testListValuedArrayTakesTheFirstValue(): void
    {
        $headers = HookHeaders::fromHeaders(['x-ankusa-id' => ['first', 'second'], 'x-ankusa-source' => ['a', 'b']]);

        self::assertSame('first', $headers->id);
        self::assertSame('a', $headers->source);
    }

    public function testNullValuedArrayEntriesAreSkipped(): void
    {
        $headers = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'x-ankusa-source' => null,
            'x-ankusa-tenant' => null,
            'content-type' => null,
        ]);

        self::assertSame('01a0', $headers->id);
        self::assertSame('', $headers->source);
        self::assertNull($headers->tenant);
        self::assertNull($headers->contentType);
    }

    public function testUnnormalizedHeaderNamesAreLowercased(): void
    {
        $headers = HookHeaders::fromHeaders([
            'X-ANKUSA-ID' => '01a0',
            'X-Ankusa-Source' => 'stripe',
            'X-ANKUSA-TENANT' => 'acme',
            'Content-Type' => 'text/plain',
        ]);

        self::assertSame('01a0', $headers->id);
        self::assertSame('stripe', $headers->source);
        self::assertSame('acme', $headers->tenant);
        self::assertSame('text/plain', $headers->contentType);
    }

    public function testOnlyTheIdIsPresent(): void
    {
        $headers = HookHeaders::fromHeaders(['x-ankusa-id' => '01a0']);

        self::assertSame('01a0', $headers->id);
        self::assertSame('', $headers->source);
        self::assertNull($headers->tenant);
        self::assertNull($headers->contentType);
    }

    public function testMissingIdIsRejected(): void
    {
        $err = self::expectMissingId(static fn(): HookHeaders => HookHeaders::fromHeaders(['x-ankusa-source' => 'stripe']));

        self::assertSame('missing x-ankusa-id header', $err->getMessage());
    }

    public function testEmptyIdIsRejected(): void
    {
        $err = self::expectMissingId(static fn(): HookHeaders => HookHeaders::fromHeaders(['x-ankusa-id' => '']));

        self::assertSame('missing x-ankusa-id header', $err->getMessage());
    }

    public function testEmptyListIdIsRejected(): void
    {
        $err = self::expectMissingId(static fn(): HookHeaders => HookHeaders::fromHeaders(['x-ankusa-id' => []]));

        self::assertSame('missing x-ankusa-id header', $err->getMessage());
    }

    public function testDedupeAndReplayHeadersAreParsed(): void
    {
        $headers = HookHeaders::fromHeaders([
            'X-Ankusa-Id' => '01a0',
            'x-ankusa-source' => 'stripe',
            'X-Ankusa-Dedupe-Key' => 'evt_9',
            'X-Ankusa-Replay-Id' => 'rid-1',
        ]);

        self::assertSame('evt_9', $headers->dedupeKey);
        self::assertSame('rid-1', $headers->replayId);
    }

    public function testEmptyDedupeAndReplayHeadersAreNull(): void
    {
        $headers = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'x-ankusa-dedupe-key' => '',
            'x-ankusa-replay-id' => '',
        ]);

        self::assertNull($headers->dedupeKey);
        self::assertNull($headers->replayId);
    }

    public function testShippedIdempotencyKeyIsParsedAndEmptyIsNull(): void
    {
        $shipped = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'X-Ankusa-Idempotency-Key' => 'acme:stripe:evt_1',
        ]);
        self::assertSame('acme:stripe:evt_1', $shipped->idempotencyKey);

        $empty = HookHeaders::fromHeaders(['x-ankusa-id' => '01a0', 'x-ankusa-idempotency-key' => '']);
        self::assertNull($empty->idempotencyKey);
    }

    public function testIdempotencyKeyPrefersTheDedupeKeyAndCanIncludeTheReplay(): void
    {
        $plain = HookHeaders::fromHeaders(['x-ankusa-id' => '01a0', 'x-ankusa-source' => 'stripe']);
        self::assertSame('01a0', $plain->idempotencyKey());

        $deduped = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'x-ankusa-source' => 'stripe',
            'x-ankusa-dedupe-key' => 'evt_1',
            'x-ankusa-replay-id' => 'rid-1',
        ]);
        self::assertSame('default:stripe:evt_1', $deduped->idempotencyKey());
        self::assertSame('default:stripe:evt_1#replay:rid-1', $deduped->idempotencyKey(true));

        $tenanted = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'x-ankusa-source' => 'stripe',
            'x-ankusa-tenant' => 'acme',
            'x-ankusa-dedupe-key' => 'evt_1',
        ]);
        self::assertSame('acme:stripe:evt_1', $tenanted->idempotencyKey());
    }

    public function testTheShippedIdempotencyKeyWinsOverRecomputingIt(): void
    {
        $headers = HookHeaders::fromHeaders([
            'x-ankusa-id' => '01a0',
            'x-ankusa-source' => 'stripe',
            'x-ankusa-tenant' => 'globex',
            'x-ankusa-dedupe-key' => 'evt_1',
            'x-ankusa-replay-id' => 'rid-1',
            'x-ankusa-idempotency-key' => 'acme:stripe:evt_1',
        ]);

        self::assertSame('acme:stripe:evt_1', $headers->idempotencyKey());
        self::assertSame('acme:stripe:evt_1#replay:rid-1', $headers->idempotencyKey(true));
    }

    /* --- helpers ------------------------------------------------------------ */

    /**
     * @return array{id: string, source: string, tenant: ?string, contentType: ?string, dedupeKey: ?string, replayId: ?string, idempotencyKey: ?string}
     */
    private static function flatten(HookHeaders $headers): array
    {
        return [
            'id' => $headers->id,
            'source' => $headers->source,
            'tenant' => $headers->tenant,
            'contentType' => $headers->contentType,
            'dedupeKey' => $headers->dedupeKey,
            'replayId' => $headers->replayId,
            'idempotencyKey' => $headers->idempotencyKey,
        ];
    }

    private static function expectMissingId(\Closure $call): MissingHookIdError
    {
        try {
            $call();
        } catch (MissingHookIdError $err) {
            return $err;
        }

        self::fail('expected MissingHookIdError');
    }
}
