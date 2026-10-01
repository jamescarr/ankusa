<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * `409 source_exists`: a source with that name already exists.
 */
final class SourceConflictError extends SourcesError {}
