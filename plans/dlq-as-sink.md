# The dead-letter queue becomes a configured sink

## Context
Today every give-up is written to a node-local file, `<data_dir>/<instance>/dlq/dlq.log`, by `Ankusa.Dispatch.DLQ.write/3`, called in the pipeline process. `GET /v1/dlq` and `POST /v1/dlq/replay` read that file.

In a real deployment the dead-letter destination should be infrastructure the operator already runs: a Kafka topic, a NATS subject, a RabbitMQ exchange, or an HTTP endpoint. Every `Ankusa.Sink` already means "durably accepted" on `:ok`:
- HTTP `2xx`;
- Kafka `acks=all`;
- JetStream ack;
- publisher confirm.

So any sink can hold dead letters. End state:
- The instance names one sink as its dead-letter destination, `dispatch.dead_letter`.
- The local file is just the default sink implementation, `Ankusa.Sink.DeadLetterLog`.
- Listing and replay work only when that local sink is configured.

## Approach

### B1. Config
- `lib/ankusa/config.ex`: add `dead_letter: {Ankusa.Sink.DeadLetterLog, []}` to the `dispatch` defaults, with the comment `# {module, opts} implementing Ankusa.Sink: where give-ups go`. `Config.new/1` already deep-merges `:dispatch`.
- Instance-level only. There is no per-source override: one dead-letter destination per instance keeps replay and alerting in one place.

### B2. `Ankusa.Sink.DeadLetterLog` replaces `Ankusa.Dispatch.DLQ`
- Move `lib/ankusa/dispatch/dlq.ex` to `lib/ankusa/sink/dead_letter_log.ex`, module `Ankusa.Sink.DeadLetterLog`, `@behaviour Ankusa.Sink`.
  - `deliver(env, %{dead_letter: dl} = ctx, _opts)` appends `%{envelope: env, reason: {:sink, dl.sink, dl.reason}, at: dl.at}` with `sync: true` to `Config.path(Ankusa.config(ctx.instance), "dlq/dlq.log")`.
    - The on-disk record shape and path are unchanged, so existing files stay readable and the admin JSON is unchanged.
    - Wrap the append in `:global.trans({{__MODULE__, path}, self()}, fn -> DurableLog.append(…) end, [node()])`. Delivery now runs in concurrent dispatch tasks, and appends must stay serialized; the old code serialized them by running in the pipeline process.
    - Returns `:ok`.
  - `deliver(_env, _ctx, _opts)` without `:dead_letter` returns `{:error, :dead_letter_only}`.
  - `ordering_key(_env, _opts)` returns `nil`.
  - `entries(Config.t())` is unchanged (`DurableLog.read(path, safe: false)`).
- Callers to migrate: `lib/ankusa/dispatch.ex:9,24`, `lib/ankusa/admin/router.ex:93`, `lib/ankusa/dispatch/pipeline.ex:160` (removed in B3), `test/ankusa/dispatch_test.exs:160,177,242`, and `test/ankusa/admin/router_test.exs:93-184`.
  - The router tests seed entries by calling `DeadLetterLog.deliver(env, %{instance: inst, source_id: env.source_id, tenant_id: env.tenant_id, attempt: 1, dead_letter: %{sink: Ankusa.Sink.Http, reason: r, attempts: 1, at: System.system_time(:millisecond)}}, [])`. Put this in a private `dead_letter(config, env, reason)` test helper.
  - `grep -rn "Dispatch.DLQ\|DLQ\." lib test` must return nothing afterwards.

### B3. Dispatch delivers give-ups to the configured sink, inside the job
- `lib/ankusa/sink.ex` `@type ctx`: add `optional(:dead_letter) => %{sink: module(), reason: term(), attempts: pos_integer(), at: integer()}`. Document it as "present only when this delivery is a dead letter".
- `lib/ankusa/dispatch/pipeline.ex` `deliver/5`, `:give_up` branch: after the existing `[:dispatch, :stop]` emit (`result: :dlq`), call `dead_letter(job, instance, config, max_sleep, {mod, reason}, attempt, 1)` and return its result.
- New private `dead_letter/7`:
  1. `{dmod, dopts} = config.dispatch.dead_letter`.
  2. `ctx = ctx(job, instance, 1) |> Map.put(:dead_letter, %{sink: mod, reason: reason, attempts: attempts, at: System.system_time(:millisecond)})`. `at` is computed once, before the loop.
  3. `safe_deliver(dmod, job.env, ctx, dopts)`:
     - `:ok` → return `{:dead, {:sink, mod, reason}}`.
     - `{:error, e}` → emit `[:dispatch, :dead_letter_error]` with metadata `%{instance: instance, source_id: job.env.source_id, sink: dmod, reason: e}`, then `sleep(min(1_000 * 2 ** (n - 1), 30_000), max_sleep)` and retry with `n + 1`, **forever**.
  - The job's lane and the cursor stay held until the dead-letter sink accepts. A dead-letter outage is backpressure, never loss. No fallback to the local file.
  - Claim check: `ctx/3` already carries `job.claim` when present. Queue sinks without one check the body in themselves via `Message.encode/3`.
- `handle_info({ref, {:dead, reason}}, …)`: delete the `DLQ.write/3` call and its comment and keep the `[:dispatch, :dlq]` telemetry emit. Update the `deliver/4` comment to "Returns `:ok` or `{:dead, reason}` once the dead letter is durably accepted".
- `lib/ankusa/metrics.ex`: add counter `"ankusa.dispatch.dead_letter_errors.total"` on `[:ankusa, :dispatch, :dead_letter_error]`, tags `[:instance, :source_id, :sink]`. `lib/ankusa/telemetry.ex`: add the matching doc row.

### B4. Sinks carry dead-letter metadata
- `lib/ankusa/sink/http.ex`: when `ctx[:dead_letter]` is present, append these headers:
  - `x-ankusa-dead-letter-sink` = `inspect(sink)`;
  - `x-ankusa-dead-letter-reason` = `String.slice(inspect(reason), 0, 512)`;
  - `x-ankusa-dead-letter-attempts`;
  - `x-ankusa-dead-lettered-at` (unix ms).
  Document them in the moduledoc.
- `lib/ankusa/sink/message.ex` `encode/3`: when `ctx[:dead_letter]` is present, add `dead_letter: %{sink: inspect(sink), reason: inspect(reason), attempts: n, at: ms}` to `base`. `v` stays `1`, since adding a field is non-breaking per the moduledoc. Document it in the moduledoc example. The Kafka, NATS and RabbitMQ sinks inherit this through `Message.encode/3`; confirm each calls it with the ctx it receives.

### B5. Admin API and replay are local-only
- `lib/ankusa/admin/router.ex`: `GET /v1/dlq` and `POST /v1/dlq/replay`, after the role check, require `elem(config.dispatch.dead_letter, 0) == Ankusa.Sink.DeadLetterLog`. Otherwise they return `409 {"error":"dlq_not_local","sink":"<inspect module>"}`. Same status family as `role_not_enabled`; document it next to that paragraph in the moduledoc and in `priv/openapi` (admin spec).
- `Ankusa.Dispatch.replay/2` keeps reading `DeadLetterLog.entries/1` and delivering to the source's current sinks. Its semantics are unchanged: it re-delivers to every current sink, not only the failed one.

### B6. Server YAML
- `ankusa_server/lib/ankusa_server/config.ex`:
  - Add `dead_letter` to `@dispatch_keys`.
  - In `dispatch_section/1`, `|> put_opt(:dead_letter, dead_letter(dispatch["dead_letter"]))`.
  - `dead_letter(nil)` → `nil`, so the core default applies.
  - A map with `type: local` (and no other keys; otherwise `unknown key`) → `{Ankusa.Sink.DeadLetterLog, []}`.
  - Any other map → `sink!(map, ["dispatch", "dead_letter"])`, the same parser and types as source sinks, so `http | log | rabbitmq | kafka | nats` all work.
- `reference.yml`: under `dispatch:` add `dead_letter: {type: local}` with the comment `# where give-ups go: local (default; enables /v1/dlq + replay) or any sink type, e.g. {type: kafka, brokers: [...], topic: ankusa.dlq}`.

### B7. Docs
- `docs/delivery.md`, the DLQ section, says:
  - the dead-letter destination is a sink;
  - the default local log and its path;
  - the ctx/header/`dead_letter` field contract;
  - "an unavailable dead-letter sink stalls dispatch; it never drops";
  - listing and replay exist only for the local sink, and an external destination is replayed with that system's own tooling.
- `docs/configuration.md` gets a `dispatch.dead_letter` row.
- `docs/quickstart.md` is unchanged (the default is local).

## Critical files & anchors
- `lib/ankusa/dispatch/pipeline.ex`: `deliver/5` `:give_up` (~465), `handle_info({ref, result}…)` (~151), `ctx/3` (~483), `safe_deliver/4` (~496).
- `lib/ankusa/dispatch/dlq.ex` becomes `lib/ankusa/sink/dead_letter_log.ex`; this is where append serialization moves.
- `lib/ankusa/admin/router.ex`: `dlq_index/1`, `dlq_replay/1`, `require_role/3` (~200).
- `ankusa_server/lib/ankusa_server/config.ex`: `@dispatch_keys` (~65), `dispatch_section/1` (~373), `sink!/2` (~739).

## Verification
- `mix test test/ankusa/dispatch_test.exs`: the existing give-up/replay/raise tests pass against `DeadLetterLog.entries/1`. New tests:
  1. `"give-ups go to the configured dead-letter sink with failure metadata"`:
     - `dispatch: %{dead_letter: {CapturingSink, pid: self()}}`, an `AlwaysFail` source sink, `max_attempts: 2`.
     - Receives the env, and `ctx.dead_letter` has `sink: AlwaysFail`, `reason: :always`, `attempts: 2`.
     - `DeadLetterLog.entries(config) == []`, and the cursor advances.
  2. `"a failing dead-letter sink holds the cursor until it accepts"`: a dead-letter sink that fails twice then succeeds. After `Pipeline.tick`, it was called 3 times, `[:ankusa, :dispatch, :dead_letter_error]` fired twice, and the cursor advanced.
- `mix test test/ankusa/admin/router_test.exs`: the existing DLQ tests pass using the helper. New test: `dispatch: %{dead_letter: {Ankusa.Sink.Http, url: "http://x"}}` makes `GET /v1/dlq` return 409 `dlq_not_local`.
- `test/ankusa/sink/http_test.exs`: a ctx with `dead_letter` sends the four headers. `test/ankusa/sink/message_test.exs`: the encoded JSON has `dead_letter.attempts`.
- `ankusa_server` `config_test.exs`:
  - `dispatch.dead_letter: {type: kafka, brokers: ["k:9092"], topic: ankusa.dlq}` gives `{Ankusa.Sink.Kafka, …}`;
  - `{type: local}` gives `DeadLetterLog`;
  - absent gives the core default;
  - `{type: local, url: x}` raises `unknown key "url"`.
- End to end: `examples/quickstart` DLQ drill (`docs/quickstart.md`) still yields `{"total":1,…}` and `{"replayed":1}`. Then set `dispatch.dead_letter: {type: http, url: http://worker:8000/dlq}` in `examples/quickstart/ankusa.yml` temporarily and repeat the drill. The worker log shows a POST carrying `x-ankusa-dead-letter-sink`, and `/v1/dlq` returns 409. Revert the yml.
- AGENTS.md gate for core, `ankusa_server`, and the three adapter packages (they compile against `Ankusa.Sink`).

## Assumptions & contingencies
- One dead-letter sink per instance, with no per-source override. The owner can ask for per-source later; that would be a `Source.dead_letter` field falling back to the instance setting.
- Dead-letter retries never give up (a capped 30 s backoff). If the owner prefers bounded retries with a local-file fallback, B3 changes in `dead_letter/7` only.
- If `:global.trans/3` shows contention in `bench/core_bench.exs` (a dead-letter-heavy run), replace it with a per-instance `GenServer` registered at `Ankusa.via(instance, :dead_letter_log)` and started by `Ankusa.Instance` under the `:dispatch` role.
