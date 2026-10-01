<?php

declare(strict_types=1);

namespace Ankusa\Routes;

/**
 * The route id cannot be used to build a path.
 *
 * Raised before any request is sent. An empty id, or exactly `.` or `..`, is
 * refused: URL parsers normalize those away, so `getRoute('..')` would quietly
 * hit `/admin/` and return the list page as if it were a route. Every other id
 * is percent-encoded as one path segment, never refused.
 */
final class InvalidRouteIdError extends RoutesError {}
