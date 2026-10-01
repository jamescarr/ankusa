<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * `GET /health` reported a version other than the client's `$expectedVersion`.
 */
final class VersionMismatchError extends SourcesError {}
