defmodule Ankusa.Test.BlobStoreAdapters do
  @moduledoc """
  The four HTTP object stores (`Ankusa.BlobStore.{S3,GCS,Azure,OCI}`) pointed at a
  `Req.Test` plug, for the tests that drive them against a stub. Each gets an
  endpoint of its own, named by `tag`: the breaker (`Ankusa.BlobStore.Breaker`)
  is keyed by endpoint and bucket and is node-wide, so tests that share one would
  share its state.
  """

  alias Ankusa.BlobStore.{Azure, GCS, OCI, S3}
  alias Ankusa.Test.OCIKey

  @adapters [S3, GCS, Azure, OCI]

  @doc "The adapter modules."
  @spec all() :: [module()]
  def all, do: @adapters

  @doc "`{module, opts}` for `adapter`, its requests handled by `Req.Test` stub `stub`. `extra` overrides."
  @spec opts(module(), String.t(), atom(), keyword()) :: {module(), keyword()}
  def opts(adapter, tag, stub, extra \\ []) do
    {adapter, Keyword.merge(base(adapter, tag), [req_options: [plug: {Req.Test, stub}]] ++ extra)}
  end

  defp base(S3, tag) do
    [
      bucket: "b",
      region: "us-east-1",
      endpoint: "http://s3.#{tag}.test",
      access_key_id: "AKID",
      secret_access_key: "secret"
    ]
  end

  defp base(GCS, tag), do: [bucket: "b", endpoint: "http://gcs.#{tag}.test"]

  defp base(Azure, tag),
    do: [account_name: "acct", container: "c", endpoint: "http://azure.#{tag}.test/acct"]

  defp base(OCI, tag) do
    [
      region: "us-ashburn-1",
      namespace: "ns",
      bucket: "b",
      tenancy_ocid: "ocid1.tenancy.oc1..t",
      user_ocid: "ocid1.user.oc1..u",
      key_fingerprint: "aa:bb:cc",
      private_key: OCIKey.private_key_pem(),
      endpoint: "http://oci.#{tag}.test"
    ]
  end

  @doc "Lowercase request headers of a `Plug.Conn` as a map."
  @spec headers(Plug.Conn.t()) :: %{String.t() => String.t()}
  def headers(conn), do: Map.new(conn.req_headers)

  @doc "A conn's query parameters."
  @spec query(Plug.Conn.t()) :: %{String.t() => String.t()}
  def query(conn), do: Plug.Conn.fetch_query_params(conn).query_params
end
