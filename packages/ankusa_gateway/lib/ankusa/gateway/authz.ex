defmodule Ankusa.Gateway.Authz do
  @moduledoc """
  The policy engine: `decide/4` evaluates one policy of
  `gateway.authz.policies` for one `Ankusa.Gateway.Authz.Request`.

  ## Modes

  A policy is a list of authorizer steps and a mode:

    * `:first` — steps in order; the first `:allow` or `:deny` wins.
    * `:all` — steps in order; the first `:deny` wins; allowed when at least
      one step allowed and none denied.
    * `:any` — steps in order; the first `:allow` wins; otherwise deny.

  Every mode denies when every step abstains. A step that answers
  `{:error, {:unavailable, _}}` is handled by its `on_unavailable`: `:fail`
  (the default) ends the decision as unavailable (`503 authz_unavailable`),
  `:abstain` and `:deny` stand in for its answer. An authorizer that raises,
  exits or returns something else is unavailable too
  (`{:unavailable, {:authorizer, name}}`), logged at `:error` at most once a
  second per authorizer.

  ## Where a step runs (`c:Ankusa.Gateway.Authorizer.kind/1`)

    * `:inline` — `decide/3` in the request process, every time.
    * `:cached` — the decision cache (`Ankusa.Gateway.Cache`, `:authz`) keyed
      `{name, subject_key, action, scope}`; a miss runs `decide/3` in the
      request process and caches the answer.
    * `:remote` — the same cache; a miss goes through the authorizer's
      `Ankusa.Gateway.Caller` (coalesced, bounded, batched through
      `decide_many/3` when exported, behind a breaker), whose run caches each
      answer before returning, fresh from when the run started.

  `subject_key` fingerprints the whole principal (`Ankusa.Gateway.Authn`), so
  a changed group or claim is a miss; `scope` is the authorizer's
  `cache_scope/2`, or `{kind, space, tenant, key}`. An allow is fresh for
  `min(meta.ttl_ms || allow_ttl_ms, allow_ttl_max_ms)`, a deny or abstain for
  `deny_ttl_ms` (per authorizer, defaulting to `authz.cache`); `0` is never
  cached, and an error never is. With `authz.cache.refresh_ahead`, a `:remote`
  hit in the last fifth of its freshness is refreshed in the background. An
  entry is kept `authz.cache.serve_stale_ms` past its freshness: a call that
  then comes back unavailable answers with it.

  The whole policy has `authz.deadline_ms`.

  ## Share links

  A request authenticated by a share link (`via: :link`) is decided by the
  step named `grants` alone, and only for `:read` on an `:object`: every other
  step counts as an abstain, and any other action or resource kind is denied
  before a step runs.

  ## Telemetry

  `[:ankusa, :gateway, :authz]`: `%{duration}`, `%{instance, policy, action,
  decision: :allow | :deny | :unavailable, authorizer, cached: true | false |
  :stale}` (the deciding step's name and how its answer was had).

  The decision's meta is the deciding step's (`:scope` is what a `403` names)
  plus `:authorizer`, that step's name (`nil` when every step abstained).
  """

  require Logger

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.{Cache, Caller}
  alias Ankusa.Telemetry

  @type decision :: {:allow, map()} | {:deny, map()} | {:error, {:unavailable, term()}}

  @doc "Evaluate `policy` (a name of `gateway.authz.policies`) for `request`."
  @spec decide(atom(), String.t(), Request.t(), keyword()) :: decision()
  def decide(instance, policy, %Request{} = request, _opts \\ []) do
    started = System.monotonic_time()
    gateway = Ankusa.Gateway.config(instance)
    %{mode: mode, steps: steps} = Map.fetch!(gateway.authz.policies, policy)
    deadline = System.monotonic_time(:millisecond) + gateway.authz.deadline_ms

    {decision, cached} =
      case link_scope(request, steps) do
        :denied ->
          {{:deny, %{authorizer: nil, reason: :link}}, false}

        steps ->
          steps
          |> Stream.map(fn step ->
            {step.use, answer(instance, gateway, step, request, deadline)}
          end)
          |> evaluate(mode)
      end

    Telemetry.emit(
      [:gateway, :authz],
      %{duration: System.monotonic_time() - started},
      %{
        instance: instance,
        policy: policy,
        action: request.action,
        decision: verdict(decision),
        authorizer: authorizer(decision),
        cached: cached
      }
    )

    decision
  end

  defp verdict({:allow, _}), do: :allow
  defp verdict({:deny, _}), do: :deny
  defp verdict({:error, _}), do: :unavailable

  defp authorizer({_verdict, %{authorizer: name}}), do: name
  defp authorizer(_error), do: nil

  # A share link: only `grants`, only reading an object.
  defp link_scope(%Request{via: :link, action: :read, resource: %{kind: :object}}, steps),
    do:
      Enum.map(steps, fn step ->
        if step.use == "grants", do: step, else: %{step | use: :abstain}
      end)

  defp link_scope(%Request{via: :link}, _steps), do: :denied
  defp link_scope(_request, steps), do: steps

  # ── modes ───────────────────────────────────────────────────────────────────

  # `{decision, cached}`: `cached` is how the deciding step's answer was had.
  defp evaluate(answers, :first) do
    Enum.reduce_while(answers, {{:deny, %{authorizer: nil}}, false}, fn
      {name, {{:allow, meta}, cached}}, _acc -> {:halt, {{:allow, named(meta, name)}, cached}}
      {name, {{:deny, meta}, cached}}, _acc -> {:halt, {{:deny, named(meta, name)}, cached}}
      {_name, {:abstain, _cached}}, acc -> {:cont, acc}
      {_name, {{:error, _} = error, cached}}, _acc -> {:halt, {error, cached}}
    end)
  end

  defp evaluate(answers, :all) do
    answers
    |> Enum.reduce_while(nil, fn
      {name, {{:allow, meta}, cached}}, nil -> {:cont, {{:allow, named(meta, name)}, cached}}
      {_name, {{:allow, _meta}, _cached}}, allowed -> {:cont, allowed}
      {name, {{:deny, meta}, cached}}, _acc -> {:halt, {{:deny, named(meta, name)}, cached}}
      {_name, {:abstain, _cached}}, acc -> {:cont, acc}
      {_name, {{:error, _} = error, cached}}, _acc -> {:halt, {error, cached}}
    end)
    |> case do
      nil -> {{:deny, %{authorizer: nil}}, false}
      decision -> decision
    end
  end

  defp evaluate(answers, :any) do
    Enum.reduce_while(answers, {{:deny, %{authorizer: nil}}, false}, fn
      {name, {{:allow, meta}, cached}}, _acc ->
        {:halt, {{:allow, named(meta, name)}, cached}}

      {name, {{:deny, meta}, cached}}, {{:deny, %{authorizer: nil}}, _} ->
        {:cont, {{:deny, named(meta, name)}, cached}}

      {_name, {{:deny, _meta}, _cached}}, acc ->
        {:cont, acc}

      {_name, {:abstain, _cached}}, acc ->
        {:cont, acc}

      {_name, {{:error, _} = error, cached}}, _acc ->
        {:halt, {error, cached}}
    end)
  end

  defp named(meta, name), do: Map.put(meta, :authorizer, name)

  # ── one step ────────────────────────────────────────────────────────────────

  # `{answer, cached}`: the answer is `{:allow, meta} | {:deny, meta} | :abstain
  # | {:error, {:unavailable, _}}`, `on_unavailable` already applied.
  defp answer(_instance, _gateway, %{use: :abstain}, _request, _deadline), do: {:abstain, false}

  defp answer(instance, gateway, %{use: name, on_unavailable: on_unavailable}, request, deadline) do
    %{module: module, state: state} = Map.fetch!(gateway.authz.authorizers, name)
    ctx = %{instance: instance, name: name, deadline: deadline}

    {result, cached} =
      case module.kind(state) do
        :inline -> {safe_decide(module, request, state, ctx), false}
        :cached -> cached(instance, gateway, name, module, state, request, ctx)
        :remote -> remote(instance, gateway, name, module, state, request, deadline)
      end

    {shape(instance, name, result, on_unavailable), cached}
  end

  defp shape(instance, name, result, on_unavailable) do
    case result do
      {:ok, :allow, meta} -> {:allow, meta}
      {:ok, :deny, meta} -> {:deny, meta}
      {:ok, :abstain, _meta} -> :abstain
      {:error, {:unavailable, _}} = error -> unavailable(error, on_unavailable)
      other -> unavailable(bad_answer(instance, name, other), on_unavailable)
    end
  end

  defp unavailable(error, :fail), do: error
  defp unavailable(_error, :abstain), do: :abstain
  defp unavailable(_error, :deny), do: {:deny, %{}}

  @doc false
  # `decide/3` that never raises: a raise, an exit or a throw is returned as
  # `{:raised, _}` (a bad answer, unavailable).
  @spec safe_decide(module(), Request.t(), map(), map()) :: term()
  def safe_decide(module, request, state, ctx) do
    module.decide(request, state, ctx)
  rescue
    exception -> {:raised, exception}
  catch
    kind, reason -> {:raised, {kind, reason}}
  end

  defp bad_answer(instance, name, answer) do
    log_once_a_second(instance, name, fn ->
      "[ankusa] gateway authorizer #{name} failed: " <>
        inspect(answer, limit: 20, printable_limit: 512)
    end)

    {:error, {:unavailable, {:authorizer, name}}}
  end

  # ── the decision cache ──────────────────────────────────────────────────────

  defp cached(instance, gateway, name, module, state, request, ctx) do
    key = cache_key(name, module, state, request)
    now = System.monotonic_time(:millisecond)

    case Cache.fetch(instance, :authz, key) do
      {:ok, {result, fresh_until, _ttl}} when now < fresh_until ->
        {result, true}

      _miss ->
        result = safe_decide(module, request, state, ctx)
        store(instance, gateway, state, key, result, now)
        {result, false}
    end
  end

  defp remote(instance, gateway, name, module, state, request, deadline) do
    key = cache_key(name, module, state, request)
    now = System.monotonic_time(:millisecond)

    case Cache.fetch(instance, :authz, key) do
      {:ok, {result, fresh_until, ttl}} when now < fresh_until ->
        if gateway.authz.cache.refresh_ahead and (fresh_until - now) * 5 < ttl,
          do: Caller.refresh(instance, {:authz, name}, key, {key, %{request | conn: nil}})

        {result, true}

      stale ->
        partition = request.resource[:tenant] || request.resource.space
        # A remote authorizer decides from the principal and the resource; the
        # connection stays in the request process.
        item = {key, %{request | conn: nil}}

        case Caller.call(instance, {:authz, name}, key, partition, item, deadline) do
          {:error, {:unavailable, _}} = error -> serve_stale(stale, error)
          result -> {result, false}
        end
    end
  end

  defp serve_stale({:ok, {result, _fresh_until, _ttl}}, _error), do: {result, :stale}
  defp serve_stale(_miss, error), do: {error, false}

  @doc false
  # The decision cache key of one step for one request.
  @spec cache_key(String.t(), module(), map(), Request.t()) :: term()
  def cache_key(name, module, state, %Request{} = request) do
    scope =
      if function_exported?(module, :cache_scope, 2) do
        module.cache_scope(request, state)
      else
        %{kind: kind, space: space, key: key} = request.resource
        {kind, space, request.resource[:tenant], key}
      end

    {name, request.subject_key, request.action, scope}
  end

  @doc false
  # Cache one authorizer's `result` under `key`, fresh from `started_ms`. A
  # result that took long to come back is fresh for less, never for a whole
  # TTL from now.
  @spec store(atom(), Ankusa.Gateway.Config.t(), map(), term(), term(), integer()) :: :ok
  def store(instance, gateway, state, key, result, started_ms) do
    case ttl(gateway, state, result) do
      ttl when is_integer(ttl) and ttl > 0 ->
        fresh_until = started_ms + ttl

        keep =
          fresh_until + gateway.authz.cache.serve_stale_ms - System.monotonic_time(:millisecond)

        Cache.put(instance, :authz, key, {result, fresh_until, ttl}, keep)

      _never ->
        :ok
    end
  end

  defp ttl(gateway, state, {:ok, :allow, meta}) do
    remote = Map.get(state, :remote, %{})
    base = Map.get(remote, :allow_ttl_ms) || gateway.authz.cache.allow_ttl_ms
    min(Map.get(meta, :ttl_ms) || base, Map.get(remote, :allow_ttl_max_ms) || 60_000)
  end

  defp ttl(gateway, state, {:ok, verdict, _meta}) when verdict in [:deny, :abstain] do
    state |> Map.get(:remote, %{}) |> Map.get(:deny_ttl_ms) || gateway.authz.cache.deny_ttl_ms
  end

  defp ttl(_gateway, _state, _error), do: nil

  # ── the callers of `:remote` authorizers ────────────────────────────────────

  @doc """
  The child specs of one `Ankusa.Gateway.Caller` per `:remote` authorizer of
  `gateway`: its run decides (in batches through `decide_many/3` when the
  module exports it) and caches every answer before returning.
  """
  @spec caller_specs(Ankusa.Gateway.Config.t()) :: [Supervisor.child_spec()]
  def caller_specs(gateway) do
    for {name, %{module: module, state: state}} <- Enum.sort(gateway.authz.authorizers),
        module.kind(state) == :remote do
      remote = Map.get(state, :remote) || Ankusa.Gateway.Authz.Remote.options!(%{}, name)
      batch? = function_exported?(module, :decide_many, 3)

      {Caller,
       instance: gateway.instance,
       name: {:authz, name},
       label: "authz:#{name}",
       max_inflight: remote.max_inflight,
       queue_max: remote.queue_max,
       partition_queue_max: max(div(remote.queue_max, 4), 1),
       batch: if(batch?, do: %{max: remote.batch_max, window_ms: remote.batch_window_ms}),
       breaker: %{failures: remote.breaker_failures, open_ms: remote.breaker_open_ms},
       run: run(gateway.instance, name, module, state, remote.timeout_ms, batch?)}
    end
  end

  defp run(instance, name, module, state, timeout_ms, false) do
    fn {key, request} ->
      started = System.monotonic_time(:millisecond)
      ctx = %{instance: instance, name: name, deadline: started + timeout_ms}
      result = safe_decide(module, request, state, ctx)
      store(instance, Ankusa.Gateway.config(instance), state, key, result, started)
      result
    end
  end

  defp run(instance, name, module, state, timeout_ms, true) do
    fn items ->
      started = System.monotonic_time(:millisecond)
      ctx = %{instance: instance, name: name, deadline: started + timeout_ms}
      gateway = Ankusa.Gateway.config(instance)

      case safe_decide_many(module, Enum.map(items, &elem(&1, 1)), state, ctx) do
        results when is_list(results) and length(results) == length(items) ->
          for {{key, _request}, result} <- Enum.zip(items, results) do
            store(instance, gateway, state, key, result, started)
            result
          end

        _bad ->
          List.duplicate({:error, {:unavailable, :bad_batch}}, length(items))
      end
    end
  end

  defp safe_decide_many(module, requests, state, ctx) do
    module.decide_many(requests, state, ctx)
  rescue
    exception -> {:raised, exception}
  catch
    kind, reason -> {:raised, {kind, reason}}
  end

  # ── the rate-limited log ────────────────────────────────────────────────────

  @doc false
  # The table `Ankusa.Gateway.Supervisor` creates for `log_once_a_second/3`.
  # sobelow_skip ["DOS.BinToAtom"]
  @spec log_table(atom()) :: atom()
  def log_table(instance), do: :"ankusa_gateway_log_#{instance}"

  defp log_once_a_second(instance, key, message) do
    now = System.monotonic_time(:millisecond)
    table = log_table(instance)

    due? =
      try do
        case :ets.lookup(table, key) do
          [{^key, last}] when now - last < 1_000 -> false
          _ -> :ets.insert(table, {key, now})
        end
      rescue
        ArgumentError -> true
      end

    if due?, do: Logger.error(message)
    :ok
  end
end
