<?php

declare(strict_types=1);

namespace Ankusa\Tests\Unit\Message;

use Ankusa\Message\InvalidMessageError;
use Ankusa\Message\Message;
use PHPUnit\Framework\TestCase;

/**
 * The vectors cover the decode rules (`message.event_type`); this names the
 * three shapes a consumer sees.
 */
final class MessageTest extends TestCase
{
    private const BASE = [
        'v' => 1,
        'id' => '01a0',
        'source_id' => 'demo',
        'received_at' => 1,
        'size' => 5,
        'body_base64' => 'aGVsbG8=',
    ];

    public function testEventTypeIsNullWhenAbsent(): void
    {
        self::assertNull(Message::decode(self::json(self::BASE))->eventType);
    }

    public function testEventTypeIsNullWhenNull(): void
    {
        self::assertNull(Message::decode(self::json(self::BASE + ['event_type' => null]))->eventType);
    }

    public function testEventTypeIsTheProvidersEventName(): void
    {
        $message = Message::decode(self::json(self::BASE + ['event_type' => 'invoice.paid']));

        self::assertSame('invoice.paid', $message->eventType);
    }

    public function testEventTypeMustBeAString(): void
    {
        try {
            Message::decode(self::json(self::BASE + ['event_type' => 7]));
            self::fail('expected InvalidMessageError');
        } catch (InvalidMessageError $err) {
            self::assertSame('invalid_field', $err->errorCode);
            self::assertSame('event_type', $err->field);
        }
    }

    /**
     * @param array<string, mixed> $payload
     */
    private static function json(array $payload): string
    {
        return json_encode($payload, JSON_THROW_ON_ERROR);
    }
}
