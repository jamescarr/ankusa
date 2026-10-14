defmodule Ankusa.Scim.Filter do
  @moduledoc """
  The slice of the SCIM filter language (RFC 7644 section 3.4.2.2) identity
  providers use to look a resource up before creating it: `<attr> eq "<value>"`.

    * Users: `userName`, `externalId`, `id`. Groups: `displayName`,
      `externalId`, `id`.
    * The attribute name and the operator are case-insensitive (`username EQ
      "x"`); inside the quoted value, `\\"` and `\\\\` are the only escapes.
    * Anything else (another operator, `and`/`or`, a nested path, an attribute
      that cannot be looked up) is an error the router answers `400
      invalidFilter`.

  `userName` and `displayName` match case-insensitively; the caller folds the
  value (`Ankusa.Directory.find_user/3` does).
  """

  @type attr :: :user_name | :display_name | :external_id | :id

  @attributes %{
    user: %{"username" => :user_name, "externalid" => :external_id, "id" => :id},
    group: %{"displayname" => :display_name, "externalid" => :external_id, "id" => :id}
  }

  @filter ~r/\A\s*([A-Za-z][A-Za-z0-9]*)\s+([A-Za-z]+)\s+"((?:[^"\\]|\\.)*)"\s*\z/s

  @doc "Parse a filter for resources of `type` (`:user` or `:group`)."
  @spec parse(:user | :group, String.t()) :: {:ok, {attr(), String.t()}} | {:error, String.t()}
  def parse(type, filter) when type in [:user, :group] and is_binary(filter) do
    case Regex.run(@filter, filter) do
      [_all, attr, op, quoted] ->
        with :ok <- operator(op),
             {:ok, attr} <- attribute(type, attr),
             {:ok, value} <- unescape(quoted) do
          {:ok, {attr, value}}
        end

      nil ->
        {:error, ~s(only filters of the form: attribute eq "value" are supported)}
    end
  end

  defp operator(op) do
    if String.downcase(op) == "eq",
      do: :ok,
      else: {:error, "the operator #{inspect(op)} is not supported; only eq is"}
  end

  defp attribute(type, attr) do
    case Map.fetch(Map.fetch!(@attributes, type), String.downcase(attr)) do
      {:ok, attr} ->
        {:ok, attr}

      :error ->
        names = @attributes |> Map.fetch!(type) |> Map.keys() |> Enum.sort()

        {:error,
         "cannot filter on #{inspect(attr)}; supported attributes: #{Enum.join(names, ", ")}"}
    end
  end

  @doc """
  The characters of a quoted filter value (without its quotes): `\\"` and `\\\\`
  are unescaped, any other escape is an error.
  """
  @spec unescape(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def unescape(quoted), do: unescape(quoted, [])

  defp unescape(<<>>, acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
  defp unescape(<<"\\\"", rest::binary>>, acc), do: unescape(rest, ["\"" | acc])
  defp unescape(<<"\\\\", rest::binary>>, acc), do: unescape(rest, ["\\" | acc])

  defp unescape(<<"\\", _rest::binary>>, _acc),
    do: {:error, "unsupported escape in the filter value"}

  defp unescape(<<char::utf8, rest::binary>>, acc), do: unescape(rest, [<<char::utf8>> | acc])
  defp unescape(_invalid_utf8, _acc), do: {:error, "the filter value is not valid UTF-8"}
end
