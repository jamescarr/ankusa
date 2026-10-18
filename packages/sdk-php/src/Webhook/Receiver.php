<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

use Psr\Http\Message\MessageInterface;

/**
 * The receiver-side decision on one HTTP-sink delivery: is it signed, and what
 * is its identity.
 *
 * Ankusa's HTTP sink signs every delivery unless it is configured
 * `allow_unsigned: true`, so a receiver is closed by default: without the
 * sink's secret (or an explicit `allowUnsigned: true`) it refuses every
 * delivery. Otherwise any host that can reach the receiver's URL could inject
 * hooks.
 */
final class Receiver
{
    /**
     * Judge one delivery; `$body` must be the raw bytes received.
     *
     * With `$secrets` (the sink's secret, `whsec_` + base64 or any other string
     * used as its own bytes; several during a rotation) the delivery MUST
     * verify, and `$allowUnsigned` does not weaken that.
     *
     * @param MessageInterface|array<string, string|list<string>|null> $headers
     * @param list<string> $secrets
     *
     * @throws InvalidSignatureError (answer 401) when there is no valid signature, including
     *                               `invalid_secret` when no secret is configured and
     *                               `$allowUnsigned` is not set
     * @throws MissingHookIdError    (answer 400) when `x-ankusa-id` is absent; the signature is
     *                               judged first
     */
    public static function receive(
        MessageInterface|array $headers,
        string $body,
        array $secrets = [],
        bool $allowUnsigned = false,
        int $toleranceSeconds = Signature::DEFAULT_TOLERANCE_SECONDS,
        ?int $now = null,
    ): ReceivedDelivery {
        if ($secrets !== []) {
            Signature::verify($headers, $body, $secrets, $toleranceSeconds, $now);
            $verified = true;
        } elseif ($allowUnsigned) {
            $verified = false;
        } else {
            throw new InvalidSignatureError(
                "no secret configured: deliveries are signed, so the receiver needs the sink's secret (or allowUnsigned: true)",
                'invalid_secret',
            );
        }

        return new ReceivedDelivery(HookHeaders::fromHeaders($headers), $verified);
    }
}
