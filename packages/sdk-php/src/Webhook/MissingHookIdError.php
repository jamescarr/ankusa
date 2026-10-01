<?php

declare(strict_types=1);

namespace Ankusa\Webhook;

use Ankusa\AnkusaException;

/**
 * Thrown by {@see HookHeaders::fromHeaders()} when `x-ankusa-id` is absent or
 * empty.
 *
 * Every other Ankusa header is optional; this one is the identity a receiver
 * dedupes on, so a delivery without it is a framework bug, not a malformed
 * but tolerable request.
 */
final class MissingHookIdError extends \UnexpectedValueException implements AnkusaException {}
