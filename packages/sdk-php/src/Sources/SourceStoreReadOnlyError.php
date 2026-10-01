<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * `409 source_store_read_only`: the deployment's source store is a static seed,
 * so writes are impossible.
 */
final class SourceStoreReadOnlyError extends SourcesError {}
