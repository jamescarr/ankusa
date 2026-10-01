<?php

declare(strict_types=1);

namespace Ankusa\ClaimCheck;

/**
 * The gateway returned `404`: no such object, expired by retention or never
 * written.
 */
final class ClaimNotFoundError extends ClaimCheckError {}
