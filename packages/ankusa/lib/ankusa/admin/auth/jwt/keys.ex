defmodule Ankusa.Admin.Auth.JWT.Keys do
  @moduledoc """
  The signing keys `Ankusa.Admin.Auth.JWT` verifies against. One server holds
  the keys of several *realms*, each a `jwt` block of its own. The instance
  starts one (`server: :auth_keys`, the default) for every control plane that
  runs `mode: :jwt`, each section a realm; a library that verifies tokens of
  its own issuers (`ankusa_gateway`) starts another under its own `server`
  name with explicit `realms:`.

  Each realm's keys are the `jwt.keys` it was configured with, loaded at
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

  Options of `start_link/1`:

    * `:instance` (required).
    * `:server` — the name it registers under (`Ankusa.via(instance, server)`)
      and the name of its table; default `:auth_keys`.
    * `:realms` — `%{realm => jwt}`; default: every control-plane section in
      `mode: :jwt` in the instance's config. A static key of an explicit realm
      is labelled `"<jwt.label>.keys[<i>]"` in errors (`jwt.label` defaults to
      `inspect(realm)`).
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
    server = Keyword.get(opts, :server, :auth_keys)
    args = {instance, server, Keyword.get(opts, :realms)}
    GenServer.start_link(__MODULE__, args, name: Ankusa.via(instance, server))
  end

  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    server = Keyword.get(opts, :server, :auth_keys)
    %{id: {__MODULE__, instance, server}, start: {__MODULE__, :start_link, [opts]}}
  end

  # The instance and server names are operator config (one atom each, never
  # request input).
  # sobelow_skip ["DOS.BinToAtom"]
  defp table(instance, server), do: :"ankusa_#{server}_#{instance}"

  # ── reads (the request path) ──────────────────────────────────────────────

  @doc """
  The keys that may have signed a token with this `kid` (`nil`: the token has
  none): the keys with that id and the keys with no id. `[]` while nothing is
  loaded.
  """
  @spec candidates(atom(), term(), String.t() | nil, atom()) :: [JOSE.JWK.t()]
  def candidates(instance, realm, kid, server \\ :auth_keys) do
    table = table(instance, server)
    keys = rows(table, {realm, :static}) ++ rows(table, {realm, :remote})

    case kid do
      nil -> keys
      kid -> Enum.filter(keys, &(key_id(&1) in [kid, nil]))
    end
  end

  defp rows(table, key) do
    case :ets.lookup(table, key) do
      [{^key, keys}] -> keys
      [] -> []
    end
  rescue
    # No table: the process is not up (yet). Nothing is trusted.
    ArgumentError -> []
  end

  defp key_id(%JOSE.JWK{fields: fields}), do: Map.get(fields, "kid")

  @doc """
  Ask for the JWKS of `realm` to be fetched again, unless it was fetched less
  than `jwt.min_refresh_ms` ago. Never blocks.
  """
  @spec refresh(atom(), term(), atom()) :: :ok
  def refresh(instance, realm, server \\ :auth_keys),
    do: GenServer.cast(Ankusa.via(instance, server), {:refresh, realm})

  @doc "Fetch the JWKS of `realm` now, whatever `jwt.min_refresh_ms` says."
  @spec reload(atom(), term(), atom()) :: :ok | {:error, term()}
  def reload(instance, realm, server \\ :auth_keys),
    do: GenServer.call(Ankusa.via(instance, server), {:reload, realm}, 15_000)

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init({instance, server, realms}) do
    realms = realms || config_realms(instance)

    table =
      :ets.new(table(instance, server), [:named_table, :protected, :set, read_concurrency: true])

    state = %{table: table, sections: realms, last_fetch: %{}, timers: %{}}

    for {realm, jwt} <- realms do
      keys =
        jwt.keys
        |> Enum.with_index()
        |> Enum.map(fn {key, index} ->
          Auth.public_key!(key, "#{label(realm, jwt)}.keys[#{index}]")
        end)

      :ets.insert(table, {{realm, :static}, keys})
    end

    {:ok, state, {:continue, :fetch}}
  end

  # The control planes of the instance's config in `mode: :jwt`, one realm
  # each, labelled by their section as before realms existed.
  defp config_realms(instance) do
    config = Ankusa.config(instance)

    for section <- @sections,
        %{mode: :jwt, jwt: jwt} <- [Auth.auth_config(config, section)],
        into: %{},
        do: {section, Map.merge(jwt, %{label: "#{section}.jwt", log_label: "#{section}.auth"})}
  end

  defp label(realm, jwt), do: Map.get_lazy(jwt, :label, fn -> inspect(realm) end)

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
            "[ankusa] could not fetch the JWKS of #{log_label(section, jwt)}: " <>
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

  # The config-derived realms keep the wording of the warning they always had
  # (`admin.auth`); an explicit realm is named by its label.
  defp log_label(realm, jwt), do: Map.get_lazy(jwt, :log_label, fn -> label(realm, jwt) end)

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
