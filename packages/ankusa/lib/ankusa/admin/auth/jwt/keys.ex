defmodule Ankusa.Admin.Auth.JWT.Keys do
  @moduledoc """
  The signing keys `Ankusa.Admin.Auth.JWT` verifies against, for every control
  plane of an instance that runs `mode: :jwt`. Started with the instance (only
  when some listener needs it).

  Each listener's keys are the `jwt.keys` it was configured with, loaded at
  start, plus the keys of its `jwt.jwks_url`:

    * fetched right after start, without delaying it, and again every
      `jwt.refresh_ms`;
    * refetched early, at most once per `jwt.min_refresh_ms`, when a token names
      a `kid` that is not in the set (the provider rotated its keys);
    * kept, not dropped, when a refetch fails: a provider outage must not lock
      operators out of a running node. The failure is a warning and the next
      attempt comes sooner. Until the first fetch succeeds there are no keys and
      every token is refused.

  Only public RSA, EC and OKP keys are kept; a JWKS entry for encryption
  (`use: "enc"`), a shared key or a private key is skipped. The set is read from
  an ETS table by the request, never through this process.
  """

  use GenServer

  require Logger

  alias Ankusa.Admin.Auth
  alias Ankusa.HttpClient

  @sections [:admin, :routes_admin, :claim_check]
  @max_response_bytes 1_048_576
  @max_keys 100
  @fetch_timeout_ms 5_000
  # After a failed fetch: sooner than `refresh_ms`, never later than this.
  @retry_ms 30_000

  # ── lifecycle ─────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :auth_keys))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp table(instance), do: :"ankusa_auth_keys_#{instance}"

  # ── reads (the request path) ──────────────────────────────────────────────

  @doc """
  The keys that may have signed a token with this `kid` (`nil`: the token has
  none): the keys with that id and the keys with no id. `[]` while nothing is
  loaded.
  """
  @spec candidates(atom(), Auth.section(), String.t() | nil) :: [JOSE.JWK.t()]
  def candidates(instance, section, kid) do
    keys = rows(instance, {section, :static}) ++ rows(instance, {section, :remote})

    case kid do
      nil -> keys
      kid -> Enum.filter(keys, &(key_id(&1) in [kid, nil]))
    end
  end

  defp rows(instance, key) do
    case :ets.lookup(table(instance), key) do
      [{^key, keys}] -> keys
      [] -> []
    end
  rescue
    # No table: the process is not up (yet). Nothing is trusted.
    ArgumentError -> []
  end

  defp key_id(%JOSE.JWK{fields: fields}), do: Map.get(fields, "kid")

  @doc """
  Ask for the JWKS of `section` to be fetched again, unless it was fetched less
  than `jwt.min_refresh_ms` ago. Never blocks.
  """
  @spec refresh(atom(), Auth.section()) :: :ok
  def refresh(instance, section),
    do: GenServer.cast(Ankusa.via(instance, :auth_keys), {:refresh, section})

  @doc "Fetch the JWKS of `section` now, whatever `jwt.min_refresh_ms` says."
  @spec reload(atom(), Auth.section()) :: :ok | {:error, term()}
  def reload(instance, section),
    do: GenServer.call(Ankusa.via(instance, :auth_keys), {:reload, section}, 15_000)

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(instance) do
    config = Ankusa.config(instance)

    sections =
      for section <- @sections,
          %{mode: :jwt, jwt: jwt} <- [Auth.auth_config(config, section)],
          into: %{},
          do: {section, jwt}

    table = :ets.new(table(instance), [:named_table, :protected, :set, read_concurrency: true])
    state = %{table: table, sections: sections, last_fetch: %{}, timers: %{}}

    for {section, jwt} <- sections do
      keys =
        jwt.keys
        |> Enum.with_index()
        |> Enum.map(fn {key, index} -> Auth.public_key!(key, "#{section}.jwt.keys[#{index}]") end)

      :ets.insert(table, {{section, :static}, keys})
    end

    {:ok, state, {:continue, :fetch}}
  end

  @impl true
  def handle_continue(:fetch, state) do
    state =
      Enum.reduce(state.sections, state, fn
        {section, %{jwks_url: url}}, state when is_binary(url) -> elem(fetch(state, section), 1)
        _static_only, state -> state
      end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:refresh, section}, state) do
    now = System.monotonic_time(:millisecond)

    with %{jwks_url: url, min_refresh_ms: min} when is_binary(url) <- state.sections[section],
         true <- now - Map.get(state.last_fetch, section, now - min - 1) >= min do
      {_result, state} = fetch(state, section)
      {:noreply, state}
    else
      _ -> {:noreply, state}
    end
  end

  @impl true
  def handle_call({:reload, section}, _from, state) do
    case state.sections[section] do
      %{jwks_url: url} when is_binary(url) ->
        {result, state} = fetch(state, section)
        {:reply, result, state}

      _ ->
        {:reply, {:error, :no_jwks_url}, state}
    end
  end

  @impl true
  def handle_info({:fetch, section}, state) do
    {_result, state} = fetch(state, section)
    {:noreply, state}
  end

  # One fetch of `section`'s JWKS, then the next one scheduled.
  defp fetch(state, section) do
    jwt = Map.fetch!(state.sections, section)

    {result, delay} =
      case fetch_jwks(jwt.jwks_url) do
        {:ok, keys} ->
          :ets.insert(state.table, {{section, :remote}, keys})
          {:ok, jwt.refresh_ms}

        {:error, reason} ->
          Logger.warning(
            "[ankusa] could not fetch the JWKS of #{section}.auth: " <>
              inspect(reason, limit: 10, printable_limit: 200) <> "; keeping the keys it has"
          )

          {{:error, reason}, min(jwt.refresh_ms, @retry_ms)}
      end

    if timer = state.timers[section], do: Process.cancel_timer(timer)
    timer = Process.send_after(self(), {:fetch, section}, delay)

    state =
      state
      |> put_in([:timers, section], timer)
      |> put_in([:last_fetch, section], System.monotonic_time(:millisecond))

    {result, state}
  end

  defp fetch_jwks(url) do
    case HttpClient.request(:get, url, [{"accept", "application/json"}], nil, @fetch_timeout_ms,
           max_response_bytes: @max_response_bytes
         ) do
      {:ok, 200, body} when is_binary(body) -> parse(body)
      {:ok, 200, {:truncated, _bytes}} -> {:error, :too_large}
      {:ok, status, _body} -> {:error, {:status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse(body) do
    with {:ok, %{"keys" => entries}} when is_list(entries) <- JSON.decode(body),
         [_ | _] = keys <- entries |> Enum.take(@max_keys) |> Enum.flat_map(&usable/1) do
      {:ok, keys}
    else
      {:ok, _other} -> {:error, :invalid_jwks}
      {:error, _reason} -> {:error, :invalid_jwks}
      [] -> {:error, :no_usable_keys}
    end
  end

  defp usable(%{"use" => "enc"}), do: []

  defp usable(entry) when is_map(entry) do
    [Auth.public_key!(entry, "jwks key")]
  rescue
    ArgumentError -> []
  end

  defp usable(_other), do: []
end
