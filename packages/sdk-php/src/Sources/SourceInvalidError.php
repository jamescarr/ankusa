<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * `400`, or a bad tenant/name caught before any request: the server rejected
 * the spec, or the identifier isn't `^[A-Za-z0-9_-]{1,64}$`.
 *
 * `getMessage()` is the server's own `message` (for a rejected spec) or its
 * `error` code (for a bad tenant/name, which has no message).
 */
final class SourceInvalidError extends SourcesError {}
