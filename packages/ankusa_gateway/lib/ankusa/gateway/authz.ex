defmodule Ankusa.Gateway.Authz do
  @moduledoc """
  The policy engine: `decide/4` evaluates one policy of
  `gateway.authz.policies` for one `Ankusa.Gateway.Authz.Request`.

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

  The decision's meta is the deciding step's (`:scope` is what a `403` names)
  plus `:authorizer`, that step's name (`nil` when every step abstained).
  """

  require Logger

  alias Ankusa.Gateway.Authz.Request

  # Until `gateway.authz.deadline_ms` exists: what a step is told it has.
  @deadline_ms 250

  @type decision :: {:allow, map()} | {:deny, map()} | {:error, {:unavailable, term()}}

  @doc "Evaluate `policy` (a name of `gateway.authz.policies`) for `request`."
  @spec decide(atom(), String.t(), Request.t(), keyword()) :: decision()
  def decide(instance, policy, %Request{} = request, _opts \\ []) do
    %{authz: %{policies: policies, authorizers: authorizers}} = Ankusa.Gateway.config(instance)
    %{mode: mode, steps: steps} = Map.fetch!(policies, policy)
    deadline = System.monotonic_time(:millisecond) + @deadline_ms

    steps
    |> Stream.map(fn step ->
      {step.use, answer(instance, step, authorizers, request, deadline)}
    end)
    |> evaluate(mode)
  end

  # ── modes ───────────────────────────────────────────────────────────────────

  defp evaluate(answers, :first) do
    Enum.reduce_while(answers, {:deny, %{authorizer: nil}}, fn
      {name, {:allow, meta}}, _acc -> {:halt, {:allow, Map.put(meta, :authorizer, name)}}
      {name, {:deny, meta}}, _acc -> {:halt, {:deny, Map.put(meta, :authorizer, name)}}
      {_name, :abstain}, acc -> {:cont, acc}
      {_name, {:error, _} = error}, _acc -> {:halt, error}
    end)
  end

  defp evaluate(answers, :all) do
    answers
    |> Enum.reduce_while(nil, fn
      {name, {:allow, meta}}, nil -> {:cont, {:allow, Map.put(meta, :authorizer, name)}}
      {_name, {:allow, _meta}}, allowed -> {:cont, allowed}
      {name, {:deny, meta}}, _acc -> {:halt, {:deny, Map.put(meta, :authorizer, name)}}
      {_name, :abstain}, acc -> {:cont, acc}
      {_name, {:error, _} = error}, _acc -> {:halt, error}
    end)
    |> case do
      nil -> {:deny, %{authorizer: nil}}
      decision -> decision
    end
  end

  defp evaluate(answers, :any) do
    Enum.reduce_while(answers, {:deny, %{authorizer: nil}}, fn
      {name, {:allow, meta}}, _acc ->
        {:halt, {:allow, Map.put(meta, :authorizer, name)}}

      {name, {:deny, meta}}, {:deny, %{authorizer: nil}} ->
        {:cont, {:deny, Map.put(meta, :authorizer, name)}}

      {_name, {:deny, _meta}}, acc ->
        {:cont, acc}

      {_name, :abstain}, acc ->
        {:cont, acc}

      {_name, {:error, _} = error}, _acc ->
        {:halt, error}
    end)
  end

  # ── one step ────────────────────────────────────────────────────────────────

  # `{:allow, meta} | {:deny, meta} | :abstain | {:error, {:unavailable, _}}`,
  # `on_unavailable` already applied.
  defp answer(
         instance,
         %{use: name, on_unavailable: on_unavailable},
         authorizers,
         request,
         deadline
       ) do
    %{module: module, state: state} = Map.fetch!(authorizers, name)
    ctx = %{instance: instance, name: name, deadline: deadline}

    case call(module, request, state, ctx) do
      {:ok, :allow, meta} -> {:allow, meta}
      {:ok, :deny, meta} -> {:deny, meta}
      {:ok, :abstain, _meta} -> :abstain
      {:error, {:unavailable, _}} = error -> unavailable(error, on_unavailable)
      other -> unavailable(bad_answer(instance, name, other), on_unavailable)
    end
  end

  defp call(module, request, state, ctx) do
    module.decide(request, state, ctx)
  rescue
    exception -> {:raised, exception}
  catch
    kind, reason -> {:raised, {kind, reason}}
  end

  defp unavailable(error, :fail), do: error
  defp unavailable(_error, :abstain), do: :abstain
  defp unavailable(_error, :deny), do: {:deny, %{}}

  defp bad_answer(instance, name, answer) do
    log_once_a_second(instance, name, fn ->
      "[ankusa] gateway authorizer #{name} failed: " <>
        inspect(answer, limit: 20, printable_limit: 512)
    end)

    {:error, {:unavailable, {:authorizer, name}}}
  end

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
