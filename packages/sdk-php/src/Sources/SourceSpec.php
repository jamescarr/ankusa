<?php

declare(strict_types=1);

namespace Ankusa\Sources;

/**
 * The writable fields of a source, as submitted to `POST`/`PUT`.
 *
 * `$verify` is optional (absent means `{"type": "none"}` on the server);
 * `$sinks` is required and must be non-empty — the server validates all of this
 * exactly as the YAML config does.
 */
final readonly class SourceSpec
{
    /**
     * @param array<int, mixed>       $sinks
     * @param array<string, mixed>|null $verify
     */
    public function __construct(
        public array $sinks,
        public ?array $verify = null,
        public ?string $onVerifyFailure = null,
    ) {}

    /**
     * The JSON body for a create/update, omitting unset (`null`) fields.
     *
     * @return array<string, mixed>
     */
    public function toJson(): array
    {
        $body = ['sinks' => $this->sinks];
        if ($this->verify !== null) {
            $body['verify'] = $this->verify;
        }
        if ($this->onVerifyFailure !== null) {
            $body['on_verify_failure'] = $this->onVerifyFailure;
        }

        return $body;
    }
}
