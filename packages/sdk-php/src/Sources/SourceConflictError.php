<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * `409 source_exists`: a source with that name already exists; or
 * `409 source_has_deliveries`: a delete found the source still has undelivered
 * hooks. The body's `error` says which.
 */
final class SourceConflictError extends SourcesError {}
