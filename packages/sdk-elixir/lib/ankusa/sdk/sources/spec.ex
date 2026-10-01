defmodule Ankusa.SDK.Sources.Spec do
  @moduledoc """
  The writable fields of a source, as submitted to `PUT`/`POST`.

  `sinks` is required and must be non-empty — the server validates the spec
  exactly as it validates the YAML config. `verify` is optional; absent means
  the server's default (`{"type": "none"}`), because omitting it is not the same
  as re-sending a secret the API never returns.

  ```elixir
  %Ankusa.SDK.Sources.Spec{
    sinks: [%{"type" => "log"}],
    verify: %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    on_verify_failure: "reject"
  }
  ```
  """

  @type t :: %__MODULE__{
          sinks: [map()] | nil,
          verify: map() | nil,
          on_verify_failure: String.t() | nil
        }

  defstruct [:sinks, :verify, :on_verify_failure]

  @doc "The JSON body for a create/update, omitting unset (`nil`) fields."
  @spec to_json(t()) :: map()
  def to_json(%__MODULE__{} = spec) do
    %{}
    |> put_unless_nil("sinks", spec.sinks)
    |> put_unless_nil("verify", spec.verify)
    |> put_unless_nil("on_verify_failure", spec.on_verify_failure)
  end

  defp put_unless_nil(body, _key, nil), do: body
  defp put_unless_nil(body, key, value), do: Map.put(body, key, value)
end
