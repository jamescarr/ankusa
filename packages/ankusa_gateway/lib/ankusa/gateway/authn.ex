defmodule Ankusa.Gateway.Authn do
  @moduledoc """
  The gateway's authentication plug: runs the chain of
  `gateway.auth.authenticators` (see `Ankusa.Gateway.Authenticator`) for every
  route but `GET /health` and `GET /ready`, and assigns

    * `principal` — the `Ankusa.Principal` the first accepting authenticator
      returned;
    * `principal_via` — the `type` of that authenticator (`:token`, `:jwt`,
      `:mtls`, ...; a custom authenticator's module), or `:anonymous` for the
      empty chain;
    * `subject_key` — 16 bytes fingerprinting the whole principal (how it was
      authenticated, its id, scopes, tenants, groups and claims), computed
      once per authentication: what decision caches key on, so two principals
      that differ in anything an authorizer may read never share a decision.

  The chain is tried in order. An authenticator with no credential of its form
  in the request (`credential/2` is `:none`) is passed over without being
  asked; one that answers `:skip` hands the request to the next. All skip is
  `401 {"error":"unauthenticated"}` with `www-authenticate: Bearer
  realm="ankusa"`, as is an authenticator's refusal; one that could not decide
  is `503 {"error":"auth_unavailable"}` with a `retry-after` of 1 to 3 seconds.

  An empty chain hands every request `Ankusa.Principal.operator/0` (every
  scope, every tenant); `Ankusa.Gateway.Config` only accepts it on loopback or
  with `auth.allow_insecure: true`.
  """

  @behaviour Plug

  import Plug.Conn

  alias Ankusa.Gateway.Authenticator
  alias Ankusa.{Http, Principal}

  @exempt [["health"], ["ready"]]
  @challenge ~s(Bearer realm="ankusa")

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{path_info: path} = conn, _opts) when path in @exempt, do: conn

  def call(conn, _opts) do
    instance = Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
    authenticators = Ankusa.Gateway.config(instance).auth.authenticators

    case authenticate(conn, instance, authenticators) do
      {:ok, principal, via} ->
        conn
        |> assign(:principal, principal)
        |> assign(:principal_via, via)
        |> assign(:subject_key, subject_key(via, principal))

      {:error, :unauthenticated} ->
        conn
        |> put_resp_header("www-authenticate", @challenge)
        |> Http.send_json(401, %{error: "unauthenticated"})
        |> halt()

      {:error, :link_expired} ->
        conn |> Http.send_json(410, %{error: "link_expired"}) |> halt()

      {:error, {:unavailable, _reason}} ->
        conn
        |> put_resp_header("retry-after", Integer.to_string(Enum.random(1..3)))
        |> Http.send_json(503, %{error: "auth_unavailable"})
        |> halt()
    end
  end

  @doc """
  The 16-byte fingerprint of a principal authenticated by `via`: the first 16
  bytes of the sha256 of `{via, id, scopes, tenants, groups, claims}` in the
  external term format, deterministic.
  """
  @spec subject_key(term(), Principal.t()) :: binary()
  def subject_key(via, %Principal{} = p) do
    term = {via, p.id, p.scopes, p.tenants, p.groups, p.claims}
    binary_part(:crypto.hash(:sha256, :erlang.term_to_binary(term, [:deterministic])), 0, 16)
  end

  defp authenticate(_conn, _instance, []), do: {:ok, Principal.operator(), :anonymous}

  defp authenticate(conn, instance, authenticators) do
    authenticators
    |> Enum.with_index()
    |> Enum.reduce_while({:error, :unauthenticated}, fn {authenticator, index}, acc ->
      ctx = ctx(instance, index, authenticator)

      case authenticator.module.credential(conn, ctx) do
        :none -> {:cont, acc}
        {:ok, _credential} -> step(authenticator, conn, ctx, acc)
      end
    end)
  end

  defp step(authenticator, conn, ctx, acc) do
    case authenticator.module.authenticate(conn, ctx) do
      {:ok, %Principal{} = principal, _expires_at} ->
        {:halt, {:ok, principal, authenticator.type}}

      :skip ->
        {:cont, acc}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  @spec ctx(atom(), non_neg_integer(), map()) :: Authenticator.ctx()
  defp ctx(instance, index, %{name: name, config: config}),
    do: %{instance: instance, index: index, name: name, config: config}
end
