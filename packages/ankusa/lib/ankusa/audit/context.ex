defmodule Ankusa.Audit.Context do
  @moduledoc """
  Who is making a change, from where, and under which request: the part of an
  audit row (`Ankusa.Audit`) that the mutation itself does not know.

  Mounted as a plug behind `Ankusa.Admin.Auth` on the admin and route-management
  APIs, it builds the context from the authenticated `Ankusa.Principal`
  (`actor`), `Ankusa.Net.ClientIP` (`remote_ip`, with `routes.trusted_proxies`
  deciding whose `X-Forwarded-For` is believed) and the caller's `X-Request-Id`
  (a fresh UUIDv7 when absent or not a plain token), and answers the request id
  in the `x-request-id` response header. A change made from code, with no
  request, is `system/0`.
  """

  @behaviour Plug

  alias Ankusa.{Net, Principal}
  alias Ankusa.Net.ClientIP

  @enforce_keys [:actor]
  defstruct actor: "system", remote_ip: nil, request_id: nil

  @type t :: %__MODULE__{
          actor: String.t(),
          remote_ip: String.t() | nil,
          request_id: String.t() | nil
        }

  @max_request_id 128
  @request_id ~r/\A[A-Za-z0-9._:\-]+\z/

  @doc "The context of a change that no request made."
  @spec system() :: t()
  def system, do: %__MODULE__{actor: "system"}

  @doc "The context carried by `conn`, `system/0` when the plug did not run."
  @spec from_conn(Plug.Conn.t()) :: t()
  def from_conn(%Plug.Conn{assigns: %{audit_context: %__MODULE__{} = context}}), do: context
  def from_conn(%Plug.Conn{}), do: system()

  @doc "The context in `opts[:audit]`, `system/0` when the caller gave none."
  @spec from_opts(keyword()) :: t()
  def from_opts(opts) do
    case Keyword.get(opts, :audit) do
      %__MODULE__{} = context -> context
      nil -> system()
    end
  end

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(%Plug.Conn{} = conn, _opts) do
    instance = Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
    request_id = request_id(conn)

    context = %__MODULE__{
      actor: actor(conn),
      remote_ip: remote_ip(conn, instance),
      request_id: request_id
    }

    conn
    |> Plug.Conn.assign(:audit_context, context)
    |> Plug.Conn.put_resp_header("x-request-id", request_id)
  end

  defp actor(%Plug.Conn{assigns: %{principal: %Principal{id: id}}}), do: id
  defp actor(%Plug.Conn{}), do: "anonymous"

  defp remote_ip(conn, instance) do
    proxies =
      instance
      |> Ankusa.config()
      |> Map.fetch!(:routes)
      |> Map.fetch!(:trusted_proxies)
      |> Enum.flat_map(fn cidr ->
        case Net.parse_cidr(cidr) do
          {:ok, parsed} -> [parsed]
          {:error, _reason} -> []
        end
      end)

    case ClientIP.resolve(conn, proxies) do
      {:ok, ip} -> ip |> :inet.ntoa() |> to_string()
      :error -> nil
    end
  end

  defp request_id(conn) do
    case Plug.Conn.get_req_header(conn, "x-request-id") do
      [id] when byte_size(id) in 1..@max_request_id ->
        if Regex.match?(@request_id, id), do: id, else: Ankusa.UUIDv7.generate()

      _ ->
        Ankusa.UUIDv7.generate()
    end
  end
end
