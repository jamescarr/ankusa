Subreddit: r/PHP
Title: I built a self-hosted webhook receiver (Elixir) with a PHP SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: none
Status: ready

This is my own project, so read it as a launch post. I built Ankusa, a self-hosted webhook receiver written in Elixir that never answers `2xx` until the hook is durably accepted, then retries to your sinks, dead-letters on give-up, and replays on demand. It runs as one container and one YAML file, and your worker can be in any language.

The PHP side is `jamescarr/ankusa`, for PHP 8.3 or newer. It talks PSR-18: Guzzle is installed by default and any other PSR-18 client can be injected as the last constructor argument. Packagist reads it through the `jamescarr/ankusa-php` split mirror. It parses the `x-ankusa-*` headers off a PSR-7 message or a plain array, decodes queue messages, redeems claim-check references with a sha256 check, and gives every error an `isRetryable()` so you know whether to requeue or dead-letter. It does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client, so bring your own.

The SDK passes the same 131 shared conformance cases as the other seven SDKs.

Delivery is at-least-once, so the same hook can reach you twice. Make your receiver idempotent and dedupe on `x-ankusa-idempotency-key`. Everything is 0.x and APIs may change.

On numbers: in a Kubernetes chaos run where I deleted two Ankusa pods and a consumer pod mid-load, every phase, including chaos, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim).

```
composer require jamescarr/ankusa

function resolveBody(ClaimCheckClient $claimCheck, array $message): string
{
    try {
        return $claimCheck->redeem($message['claim'], $message['sha256']);
    } catch (ClaimCheckError $err) {
        if (! $err->isRetryable()) {
            // bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
            throw $err;
        }
        // gateway unreachable or 5xx: safe to retry
        throw $err;
    }
}
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-php/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
