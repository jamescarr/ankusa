<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

/**
 * An accepted delivery, as returned by {@see Receiver::receive()}: its identity,
 * and whether its signature verified.
 */
final readonly class ReceivedDelivery
{
    public function __construct(
        public HookHeaders $headers,
        /** `false` only for an unsigned delivery `allowUnsigned` let through. */
        public bool $verified,
    ) {}
}
