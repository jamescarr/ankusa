<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * The admin API is unreachable, timed out, or answered `5xx`. Safe to retry.
 */
final class SourcesUnavailableError extends SourcesError {}
