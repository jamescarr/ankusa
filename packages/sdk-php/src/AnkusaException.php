<?php

declare(strict_types=1);

namespace Ankusa;

/**
 * Marker for every exception this SDK throws.
 *
 * Catch this to handle anything the SDK raises; catch a specific class (or its
 * client's base error, e.g. {@see \Ankusa\ClaimCheck\ClaimCheckError}) when the
 * classification matters.
 */
interface AnkusaException extends \Throwable {}
