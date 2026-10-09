<?php

declare(strict_types=1);

namespace Ankusa;

/**
 * The SDK's version.
 *
 * The single source of truth: `mise run status`/`release:prepare`/`release:tag`
 * read it (see `.mise/lib/pkg.sh` and `.mise/lib/release.exs`), Packagist
 * derives the published version from the mirror's git tags.
 */
final class Version
{
    public const string VERSION = '0.4.0';
}
