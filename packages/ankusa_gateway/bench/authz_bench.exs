# The gateway's budgets for "permissioning must not slow the read path"
# (ADR 0005, plan M4.8):
#
#     MIX_ENV=test mix run bench/authz_bench.exs      # or: mise run bench:gateway
#
# Every case is one process timing one call with
# `System.monotonic_time(:nanosecond)`: 5 000 warm-up iterations, then 50 000
# measured. It prints p50/p99 per case and exits 1 naming each case over its
# budget:
#
#   rules           Authz.decide/4 over a `rules` authorizer of 64 rules (the
#                   last one matches) for a principal in 20 groups: p99 <= 100 us
#   cached remote   Authz.decide/4 over a `:remote` authorizer whose decision
#                   is in the cache: p99 <= 50 us
#   authn hit       Authn.call/2 with a 2 KiB JWT already in the authn cache:
#                   p99 <= 50 us
#   router overhead Router.call/2 for a 1 KiB claim from the claim read cache,
#                   one warm `jwt` authenticator and the policy [rules, cached
#                   remote], minus the same request with no authenticator and
#                   the policy [scopes]: p50 <= 100 us, p99 <= 300 us

Logger.configure(level: :warning)

defmodule Bench.RemoteStub do
  @moduledoc false
  @behaviour Ankusa.Gateway.Authorizer

  @impl true
  def init!(opts, key),
    do: Map.put(opts, :remote, Ankusa.Gateway.Authz.Remote.options!(opts, key))

  @impl true
  def kind(_state), do: :remote

  @impl true
  def decide(_request, _state, _ctx), do: {:ok, :allow, %{}}
end

defmodule Bench.Gateway do
  @moduledoc false

  alias Ankusa.Gateway.Authz.Request

  @warmup 5_000
  @iterations 50_000

  def groups, do: for(i <- 1..20, do: "group-#{i}")

  # 63 rules that do not match a claim read (another space each), then one
  # that does, by group.
  def rules do
    misses =
      for i <- 1..63 do
        %{effect: :allow, actions: [:read], spaces: ["space-#{i}"], keys: ["**"]}
      end

    misses ++
      [
        %{
          effect: :allow,
          actions: [:read],
          spaces: ["claims"],
          keys: ["{tenant}/**"],
          when: %{groups_any: ["group-20"]}
        }
      ]
  end

  def keypair do
    jwk = JOSE.JWK.generate_key({:ec, "P-256"})
    {_, public} = jwk |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()
    {jwk, Map.put(public, "kid", "bench")}
  end

  # A JWT of about 2 KiB: the padding claim fills it.
  def jwt(jwk) do
    claims = %{
      "iss" => "https://issuer.bench/",
      "aud" => "gateway",
      "sub" => "alice",
      "exp" => System.os_time(:second) + 3600,
      "scope" => "claims:read",
      "ankusa_tenants" => ["acme"],
      "groups" => groups(),
      "pad" => String.duplicate("x", 1_200)
    }

    {_, token} =
      jwk |> JOSE.JWT.sign(%{"alg" => "ES256", "kid" => "bench"}, claims) |> JOSE.JWS.compact()

    token
  end

  def jwt_authenticator(public),
    do: %{
      type: :jwt,
      issuer: "https://issuer.bench/",
      audience: "gateway",
      keys: [public],
      groups_claim: "groups"
    }

  # A core instance with no role and a gateway over it, under one supervisor.
  def start(name, gateway_opts) do
    inst = :"bench_#{name}_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), Atom.to_string(inst))
    File.rm_rf!(dir)

    config =
      Ankusa.Config.new(
        instance: inst,
        data_dir: dir,
        port: 0,
        roles: [],
        claim_check: %{read_cache_bytes: 64 * 1024 * 1024}
      )

    gateway =
      Ankusa.Gateway.Config.new(
        [instance: inst, port: 0, claims: %{integrity_check: :off}] ++ gateway_opts
      )

    {:ok, _} =
      Supervisor.start_link([{Ankusa.Instance, config}, {Ankusa.Gateway, gateway}],
        strategy: :one_for_one
      )

    inst
  end

  def request(principal, via) do
    %Request{
      principal: principal,
      via: via,
      subject_key: Ankusa.Gateway.Authn.subject_key(via, principal),
      action: :read,
      resource: %{kind: :claim, space: "claims", key: "acme/01ABC", tenant: "acme"}
    }
  end

  # `[nanoseconds]` of `fun`, after the warm-up.
  def measure(fun) do
    for _ <- 1..@warmup, do: fun.()

    for _ <- 1..@iterations do
      started = System.monotonic_time(:nanosecond)
      fun.()
      System.monotonic_time(:nanosecond) - started
    end
  end

  # `{[ns of a], [ns of b]}`, the two timed alternately: a load that comes and
  # goes during the run lands on both, not on whichever ran second.
  def measure_pair(a, b) do
    for _ <- 1..@warmup do
      a.()
      b.()
    end

    1..@iterations
    |> Enum.map(fn _ -> {time(a), time(b)} end)
    |> Enum.unzip()
  end

  defp time(fun) do
    started = System.monotonic_time(:nanosecond)
    fun.()
    System.monotonic_time(:nanosecond) - started
  end

  def percentile(samples, p) do
    sorted = Enum.sort(samples)
    Enum.at(sorted, min(length(sorted) - 1, round(length(sorted) * p) - 1))
  end

  def us(ns), do: Float.round(ns / 1000, 1)
end

alias Bench.Gateway, as: B
alias Ankusa.Gateway.{Authn, Authz, Router}

{jwk, public} = B.keypair()
token = B.jwt(jwk)

# ── the gateway under test, and its bare twin ───────────────────────────────

inst =
  B.start("full",
    auth: %{authenticators: [B.jwt_authenticator(public)]},
    authz: %{
      authorizers: %{
        "rules" => {Ankusa.Gateway.Authz.Rules, %{rules: B.rules()}},
        "remote" => {Bench.RemoteStub, %{}}
      },
      policies: %{
        "rules" => %{mode: :first, steps: ["rules"]},
        "remote" => %{mode: :first, steps: ["remote"]},
        "claims" => %{mode: :all, steps: ["rules", "remote"]}
      }
    }
  )

bare = B.start("bare", auth: %{authenticators: []})

principal = %{Ankusa.Principal.new("alice", ["claims:read"], ["acme"]) | groups: B.groups()}
request = B.request(principal, :jwt)

{:allow, _} = Authz.decide(inst, "rules", request)
{:allow, _} = Authz.decide(inst, "remote", request)

# ── the claim both read ─────────────────────────────────────────────────────

body = :crypto.strong_rand_bytes(1024)

paths =
  for gw <- [inst, bare], into: %{} do
    {:ok, %{"a" => %{ref: ref}}} =
      Ankusa.ClaimCheck.check_in(gw, "acme", [%{id: "a", body: body}])

    {gw, Ankusa.ClaimCheck.Ref.path(ref)}
  end

router = fn gw, headers ->
  opts = Router.init(instance: gw)

  fn ->
    conn =
      Enum.reduce(headers, Plug.Test.conn(:get, paths[gw]), fn {k, v}, conn ->
        Plug.Conn.put_req_header(conn, k, v)
      end)

    %{status: 200} = Router.call(conn, opts)
  end
end

authn_conn =
  Plug.Test.conn(:get, "/v1/claims/acme/x")
  |> Plug.Conn.assign(:ankusa_opts, instance: inst)
  |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)

%{assigns: %{principal: %{id: "alice"}}} = Authn.call(authn_conn, [])

# ── measure ─────────────────────────────────────────────────────────────────

rules = B.measure(fn -> {:allow, _} = Authz.decide(inst, "rules", request) end)
remote = B.measure(fn -> {:allow, _} = Authz.decide(inst, "remote", request) end)
authn = B.measure(fn -> %{assigns: %{principal: _}} = Authn.call(authn_conn, []) end)

{full, base} =
  B.measure_pair(router.(inst, [{"authorization", "Bearer " <> token}]), router.(bare, []))

overhead = fn p -> B.percentile(full, p) - B.percentile(base, p) end

results = [
  {"rules (64 rules, 20 groups)", "p99", B.percentile(rules, 0.99), 100_000},
  {"cached remote decision", "p99", B.percentile(remote, 0.99), 50_000},
  {"authn cache hit (2 KiB JWT, #{byte_size(token)} B)", "p99", B.percentile(authn, 0.99),
   50_000},
  {"router overhead (auth + authz)", "p50", overhead.(0.5), 100_000},
  {"router overhead (auth + authz)", "p99", overhead.(0.99), 300_000}
]

IO.puts("")

IO.puts(
  "rules p50 #{B.us(B.percentile(rules, 0.5))} us | cached remote p50 #{B.us(B.percentile(remote, 0.5))} us | " <>
    "authn p50 #{B.us(B.percentile(authn, 0.5))} us | router full p50 #{B.us(B.percentile(full, 0.5))} us, " <>
    "bare p50 #{B.us(B.percentile(base, 0.5))} us"
)

misses =
  for {name, p, value, budget} <- results do
    verdict = if value <= budget, do: "ok", else: "OVER"

    IO.puts(
      "#{String.pad_trailing(name, 44)} #{p} #{B.us(value)} us (budget #{B.us(budget)} us) #{verdict}"
    )

    if value > budget, do: "#{name} #{p}", else: nil
  end
  |> Enum.reject(&is_nil/1)

if misses != [] do
  IO.puts("\nover budget: #{Enum.join(misses, ", ")}")
  System.halt(1)
end
