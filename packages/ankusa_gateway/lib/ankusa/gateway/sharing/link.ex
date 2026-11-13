defmodule Ankusa.Gateway.Sharing.Link do
  @moduledoc """
  Share links v1: a URL that reads one object until it expires or its grant
  is revoked.

      /v1/objects/<space>/<key>?link=<grant id>&exp=<unix seconds>&sig=<signature>

  The signature is the unpadded base64url HMAC-SHA256, under the first of
  `gateway.sharing.signing_keys`, of the canonical string
  `urn:ankusa:link:v1:<space>:<grant id>:<exp>` (43 characters); any of the
  keys verifies one, so a key can be rotated in front of the old ones. The
  key itself is not signed: the grant id carries its sha256, and
  `Ankusa.Gateway.Authn.Link` checks it against the path.
  """

  @exp ~r/\A[1-9][0-9]{0,11}\z/
  @sig ~r/\A[A-Za-z0-9_-]{43}\z/

  @doc "The canonical string a link signs."
  @spec canonical(String.t(), String.t(), integer() | String.t()) :: String.t()
  def canonical(space, grant_id, exp), do: "urn:ankusa:link:v1:#{space}:#{grant_id}:#{exp}"

  @doc "The signature of a link, under the first key."
  @spec sign([String.t(), ...], String.t(), String.t(), integer()) :: String.t()
  def sign([key | _], space, grant_id, exp), do: mac(key, canonical(space, grant_id, exp))

  @doc "Whether `sig` is a link's signature under any of `keys`."
  @spec valid?([String.t()], String.t(), String.t(), String.t(), String.t()) :: boolean()
  def valid?(keys, space, grant_id, exp, sig) do
    canonical = canonical(space, grant_id, exp)
    Enum.any?(keys, &Ankusa.Verifier.secure_compare(mac(&1, canonical), sig))
  end

  defp mac(key, data),
    do: :hmac |> :crypto.mac(:sha256, key, data) |> Base.url_encode64(padding: false)

  @doc """
  The path (with its query) of a link to `key` in `space`: each key segment
  percent-encoded, so `Ankusa.Gateway.Spaces.key_from_segments/1` reads back
  exactly `key`.
  """
  @spec path(String.t(), String.t(), String.t(), integer(), String.t()) :: String.t()
  def path(space, key, grant_id, exp, sig) do
    encoded =
      key
      |> String.split("/")
      |> Enum.map_join("/", fn segment -> URI.encode(segment, &URI.char_unreserved?/1) end)

    query = URI.encode_query([{"link", grant_id}, {"exp", Integer.to_string(exp)}, {"sig", sig}])
    "/v1/objects/#{space}/#{encoded}?#{query}"
  end

  @doc "Whether `exp` and `sig` are in the link grammar."
  @spec grammar?(term(), term()) :: boolean()
  def grammar?(exp, sig) when is_binary(exp) and is_binary(sig),
    do: Regex.match?(@exp, exp) and Regex.match?(@sig, sig)

  def grammar?(_exp, _sig), do: false
end
