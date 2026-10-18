Subreddit: r/ruby
Title: I built a self-hosted webhook receiver (Elixir) with a Ruby SDK for the worker side — durable before 2xx, retries, DLQ, replay
Flair: none
Status: ready

This is my own project, so read it as a launch post. I built Ankusa, a self-hosted webhook receiver written in Elixir that never answers `2xx` until the hook is durably accepted, then retries to your sinks, dead-letters on give-up, and replays on demand. It runs as one container and one YAML file, and your worker can be in any language.

The Ruby side is `ankusa-sdk`, and it uses the standard library only; the gemspec declares no runtime dependency. It parses the `x-ankusa-*` headers (the snippet below maps a Rack env to the hash `Ankusa.parse_headers` wants), decodes queue messages, redeems claim-check references with a sha256 check, and gives every error a `retryable?` so you know whether to requeue or dead-letter. It does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client, so bring your own.

The SDK passes the same 131 shared conformance cases as the other seven SDKs.

Delivery is at-least-once, so the same hook can reach you twice. Make your receiver idempotent and dedupe on `x-ankusa-idempotency-key`. Everything is 0.x and APIs may change.

On numbers: in a Kubernetes chaos run where I deleted two Ankusa pods and a consumer pod mid-load, every phase, including chaos, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim).

```
gem install ankusa-sdk

def header_map(env)
  headers = env.filter_map do |key, value|
    [key.delete_prefix("HTTP_").tr("_", "-"), value] if key.start_with?("HTTP_")
  end.to_h
  headers["content-type"] = env["CONTENT_TYPE"] if env["CONTENT_TYPE"]
  headers
end

def call(env)
  hook =
    begin
      Ankusa.parse_headers(header_map(env))
    rescue Ankusa::MissingHookIdError
      return [400, {}, []]
    end

  body = env["rack.input"].read
  # hook.id, hook.source, hook.tenant, hook.content_type
  [200, {}, []]
end
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-ruby/README.md
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
