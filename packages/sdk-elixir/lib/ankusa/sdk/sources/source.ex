defmodule Ankusa.SDK.Sources.Source do
  @moduledoc """
  A stored source as the admin API reports it: identity fields plus the redacted
  spec.

  Every value here is redacted (`secret`, `password`, `token`, HTTP header
  values, URL userinfo passwords, ...), so a `Source` read back through
  `Ankusa.SDK.Sources.get_source/3` is never useful for editing: resending its
  `verify` map is not the same as resending the stored secret. Callers that hold
  the secret supply it through `Ankusa.SDK.Sources.Spec`.
  """

  @type t :: %__MODULE__{
          tenant: String.t(),
          name: String.t(),
          source_id: String.t(),
          ingest_path: String.t(),
          verify: map(),
          on_verify_failure: String.t() | nil,
          sinks: [map()]
        }

  defstruct [:tenant, :name, :source_id, :ingest_path, :verify, :on_verify_failure, :sinks]

  @doc """
  Build a source from a decoded API object.

  `tenant`, `name`, `source_id` and `ingest_path` are required binaries;
  `verify` defaults to `%{"type" => "none"}` and `sinks` to `[]` when the body
  omits them. Returns `:error` when a required field is missing or malformed.
  """
  @spec from_json(term()) :: {:ok, t()} | :error
  def from_json(data) when is_map(data) do
    with {:ok, tenant} <- required(data, "tenant"),
         {:ok, name} <- required(data, "name"),
         {:ok, source_id} <- required(data, "source_id"),
         {:ok, ingest_path} <- required(data, "ingest_path") do
      {:ok,
       %__MODULE__{
         tenant: tenant,
         name: name,
         source_id: source_id,
         ingest_path: ingest_path,
         verify: data["verify"] || %{"type" => "none"},
         on_verify_failure: data["on_verify_failure"],
         sinks: data["sinks"] || []
       }}
    end
  end

  def from_json(_data), do: :error

  defp required(data, key) do
    case data[key] do
      value when is_binary(value) -> {:ok, value}
      _other -> :error
    end
  end
end
