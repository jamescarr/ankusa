# Ankusa launch kit

Six blog drafts, one Elixir Forum post, eight per-language Reddit posts, and a
claims register that every number and guarantee in the copy traces to. Nothing
here is published; every blog file has `draft: true`.

- Voice: first person, builder's story, honest about limits.
- Every claim: [`claims.md`](claims.md). If a sentence is not in the register,
  add a sourced row or cut the sentence.
- `{{BLOG_URL}}` is the placeholder for your blog's base URL (for example
  `https://example.com/blog`). Replace it everywhere before publishing.

## Launch gates

All of these must be true before anything is posted.

|#|Gate|How to close it|State at 2026-10-08|
|---|---|---|---|
|1|Core `[Unreleased]` is released. Replay jobs (`POST /v1/replays`), the `x-ankusa-idempotency-key` header, ingest dedupe, the 6-hour retry budget, quarantine release, and failure domains are on `main` and in `quickstart.md`, but not in the 0.4.0 tag (`packages/ankusa/CHANGELOG.md:12-296`).|`mise run release:prepare minor ankusa ankusa_rabbitmq ankusa_kafka ankusa_nats ankusa_redis ankusa_server`, merge the PR, then `mise run release:tag ankusa`, `mise run release:watch ankusa`; after core is live tag the four adapters and `ankusa_server` the same way and run `mise run release:verify` on each. Pre-tag gate: `mise run e2e` passing locally. Flow and ordering: `docs/releasing.md:28-91`.|Open. Hex and the image are at 0.4.0 (`mise run status`: `ankusa` 6 commits since the tag).|
|2|The seven published SDKs are released. `decode_message` (size, sha256 and tenant integrity checks), the `idempotency_key` helpers, and the replay admin client are in each SDK's `[Unreleased]` changelog section, not in 0.3.0. Post 05 and every `sdks/*.md` post describe them.|`mise run release:prepare minor sdk-typescript sdk-python sdk-rust sdk-ruby sdk-go sdk-php sdk-elixir`, merge, then `release:tag` / `release:watch` / `release:verify` per SDK (`docs/releasing.md`, one section per SDK kind). Then update the versions in `claims.md` §Versions.|Open. All seven are 0.3.0 with 3–4 commits since the tag. **This reverses the launch plan's "do not block launch on an SDK release"; that sentence only held for the README install text.**|
|3|Step 0 README fixes merged (`sdk-typescript`, `sdk-python`, `sdk-ruby`, `sdk-php`, `sdk-rust`: the "not published yet" install text is gone, the Rust conformance count is no longer hard-coded). The READMEs ship inside the packages, so GitHub shows the fixed text before the SDK re-release in gate 2 puts it on the registries.|Merge this branch.|Done on this branch.|
|4|Every command in Post 01 produces the documented output on launch day.|`README.md:25-36` (`docker run … jamescarr/ankusa:edge` + `curl`), then `examples/quickstart`: `docker compose up --build -d --wait` and the `docs/quickstart.md` drills (short outage, long outage, DLQ, `POST /v1/replays`). If `POST /v1/replays` returns 404 on `:edge`, gate 1 is not met: record it here, do not change the post.|See §Smoke results below.|
|5|`{{BLOG_URL}}` replaced in every file.|`grep -rn '{{BLOG_URL}}' marketing/` returns nothing.|Open (expected until publish).|
|6|Java only: `sdk-java` released.|`mise run release:prepare 0.3.0 sdk-java`, merge, `release:tag` / `release:watch` / `release:verify sdk-java`; needs the one-time Sonatype and GPG setup in `docs/releasing.md`. `sdks/java.md` stays unposted until `mise run status` shows `sdk-java … yes`.|Open. `sdk-java` is 0.0.0, unpublished.|

## Smoke results (2026-10-08)

Run against `jamescarr/ankusa:edge` (image created 2026-10-08T13:15Z) from this branch at `143aa54`. Ports 4000 and 4002 on the machine were held by an unrelated container, so every command used host ports 14000 and 14002, `ankusa-mkt-smoke*` names, and an isolated compose project (`-p ankusa-mkt-smoke-qs`, port override file); nothing else was touched and everything was removed afterwards. Otherwise the blocks ran as written in Post 01.

- `docker run` block from `README.md:25-36`, including `-e ANKUSA_ADMIN_IP=0.0.0.0`: `HTTP 201`, `{"id":"01a1…","status":"accepted"}`; boot log `[ankusa] store at /var/lib/ankusa/default/store. Durable to power loss on THIS host only.`; `GET /health` on the published admin port answers `{"status":"ok","version":"0.4.0",…}`; `GET /v1/replays` answers `{"replays":[]}`.
- The same block **without** `-e ANKUSA_ADMIN_IP=0.0.0.0`, run earlier against the same `:edge` image, left the published admin port unusable (`curl` got an empty reply) because the admin API binds `127.0.0.1` inside the container. That is why the posts carry the flag.
- `examples/quickstart` (`docker compose up --build -d --wait`) and the `docs/quickstart.md` drills: first hook delivered to the worker; short outage: `evt_2` delivered after the worker restarted; long outage: `GET /v1/dlq` showed `"total":1` (reason `nxdomain`, the worker being down), `POST /v1/replays` answered `202` with `state: running`, the job later read `state: done, moved: 1, delivered: 1`, and the worker log showed `evt_3`.
- `/asyncapi.json` answered `200`; `GET /v1/quarantine` answered `{"entries":[]}`.
- `POST /v1/replays` exists on `:edge`, so gate 1 is about the Hex and Docker `0.4.0` tags, not about `:edge`.
- `mise run check:links` passes. Not run here: `mise run e2e` (the kind + Oban run behind Posts 02 and 03; it is the pre-tag gate in `docs/releasing.md`), the SDK suites, and any provider posting.

## Calendar

`LAUNCH_DATE = 2026-10-13` (a Tuesday). Every date below is an offset; if a gate
slips, move all of them by the same number of days and keep the order. Blog
`date:` frontmatter already carries the offset dates.

|Day|Date|Piece|Channel|
|---|---|---|---|
|L+0|2026-10-13|[`blog/01-ankusa-launch.md`](blog/01-ankusa-launch.md)|blog|
|L+0|2026-10-13|[`forums/elixirforum.md`](forums/elixirforum.md)|elixirforum.com → Your Libraries & Projects › Libraries (https://elixirforum.com/c/your-libraries-os-mentoring/libraries/43), tags `webhooks`, `hex`, `otp`|
|L+0|2026-10-13|[`sdks/elixir.md`](sdks/elixir.md)|r/elixir|
|L+2|2026-10-15|[`sdks/typescript.md`](sdks/typescript.md), [`sdks/python.md`](sdks/python.md), [`sdks/go.md`](sdks/go.md), [`sdks/rust.md`](sdks/rust.md)|r/typescript (cross-post r/node), r/Python, r/golang, r/rust|
|L+3|2026-10-16|[`sdks/ruby.md`](sdks/ruby.md), [`sdks/php.md`](sdks/php.md)|r/ruby, r/PHP|
|L+7|2026-10-20|[`blog/02-never-ack-what-you-didnt-save.md`](blog/02-never-ack-what-you-didnt-save.md)|blog|
|L+14|2026-10-27|[`blog/03-killing-pods-mid-run.md`](blog/03-killing-pods-mid-run.md)|blog|
|L+21|2026-11-03|[`blog/04-megabyte-webhooks-claim-check.md`](blog/04-megabyte-webhooks-claim-check.md)|blog|
|L+28|2026-11-10|[`blog/05-eight-sdks-one-conformance-suite.md`](blog/05-eight-sdks-one-conformance-suite.md)|blog|
|L+35|2026-11-17|[`blog/06-one-catch-url-per-customer.md`](blog/06-one-catch-url-per-customer.md)|blog|
|after `sdk-java` 0.3.0 is on Maven Central|n/a|[`sdks/java.md`](sdks/java.md)|r/java|

Subreddit posts are text posts, not link posts, so the body is read; each ends
with the GitHub link. Flair and self-promotion rules differ per sub: read the
sidebar before posting. If a sub has no project flair, post as plain text.

## Posting checklist (per piece)

- [ ] Claims cross-check passed: every number and guarantee traces to [`claims.md`](claims.md), nothing from its §Forbidden appears.
- [ ] Every shell block in the piece was run today and produced the documented output.
- [ ] Every link returns 200 (repo files, HexDocs, Docker Hub, the blog cross-links after `{{BLOG_URL}}` is replaced).
- [ ] First comment prepared. For Reddit: the install block plus "ask me anything about the durability model". For the blog: a link to the quickstart.
- [ ] Reply window: watch the thread for 48 hours, answer with sources from `claims.md`.
- [ ] Versions in the text match `mise run status` that day (the Hex dependency in the forum post, the SDK install lines).

## Publishing notes

- **Template syntax.** Post 01 and the Try-it footer contain `{{.State.Health.Status}}` (verbatim from `README.md`). Jekyll/Liquid, Hugo and Nunjucks treat `{{ … }}` as template syntax: wrap those code blocks in `{% raw %}…{% endraw %}` (Liquid/Nunjucks) or the equivalent escape for your generator, or the page fails to render.
- **Mermaid.** Posts 01, 03 and 04 use `mermaid` fences copied from the repo docs; your blog needs a Mermaid plugin or a pre-render step.
- **Frontmatter.** Keys are `title date slug description tags draft`; flip `draft` to `false` when you publish.
- **Forbidden terms.** `claims.md` §Forbidden lists the claims no piece may make. The register spells them out, so any grep for those terms matches `claims.md` itself; run the grep on every other file under `marketing/`.

## Files

|File|What|
|---|---|
|[`claims.md`](claims.md)|Claims register: required caveats, guarantees, SDK claims, the only measured numbers, versions, forbidden claims, doc drift found while writing it.|
|[`blog/01-ankusa-launch.md`](blog/01-ankusa-launch.md)|Launch post.|
|[`blog/02-never-ack-what-you-didnt-save.md`](blog/02-never-ack-what-you-didnt-save.md)|Group commit, one fsync, and the `503`.|
|[`blog/03-killing-pods-mid-run.md`](blog/03-killing-pods-mid-run.md)|The kind + Oban chaos run and the bug it found first.|
|[`blog/04-megabyte-webhooks-claim-check.md`](blog/04-megabyte-webhooks-claim-check.md)|Claim check for large payloads.|
|[`blog/05-eight-sdks-one-conformance-suite.md`](blog/05-eight-sdks-one-conformance-suite.md)|Eight SDKs and the shared conformance suite.|
|[`blog/06-one-catch-url-per-customer.md`](blog/06-one-catch-url-per-customer.md)|Multi-tenant catch URLs.|
|[`forums/elixirforum.md`](forums/elixirforum.md)|Elixir Forum announcement.|
|[`sdks/typescript.md`](sdks/typescript.md), [`python.md`](sdks/python.md), [`rust.md`](sdks/rust.md), [`ruby.md`](sdks/ruby.md), [`go.md`](sdks/go.md), [`php.md`](sdks/php.md), [`elixir.md`](sdks/elixir.md), [`java.md`](sdks/java.md)|One Reddit text post per language; `java.md` is gated on gate 6.|
