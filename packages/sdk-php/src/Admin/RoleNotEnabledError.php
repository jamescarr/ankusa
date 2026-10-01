<?php

declare(strict_types=1);

namespace Ankusa\Admin;

/**
 * The listener returned `409 role_not_enabled`: this node does not run the
 * role the operation needs. Ask another node; `$role` names it.
 */
final class RoleNotEnabledError extends AdminError
{
    public function __construct(string $message, public readonly ?string $role = null)
    {
        parent::__construct($message);
    }
}
