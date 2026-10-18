defmodule Ankusa.Verifier.Template do
  @moduledoc ~S"""
  The wire half of a signature scheme, shared by every verifier that reads a
  signature out of a request header: `Ankusa.Verifier.Hmac` and
  `Ankusa.Verifier.Asymmetric`.

  A scheme is any struct with the fields `signature_header`, `parse`,
  `sig_prefix`, `sig_key`, `version`, `signed` and `timestamp` (the
  `Scheme` structs of both verifiers carry them). This module knows how to

    * pull the candidate signatures, and a signature-borne timestamp, out of
      the header (`candidates/2`),
    * read the timestamp and judge it against the replay window
      (`timestamp/4`), and
    * build the exact bytes that were signed from the `signed` template over
      `{body}`, `{ts}` and `{header:NAME}` (`signed_bytes/3`).

  What is *done* with those (an HMAC, an Ed25519 or RSA verification) is the
  verifier's business. Every failure is a reason from the
  `Ankusa.Verifier` vocabulary (`:missing_signature`, `:malformed_signature`,
  `:timestamp_out_of_tolerance`).
  """

  alias Ankusa.Envelope
  alias Ankusa.Verifier

  @doc """
  The candidate signatures in `scheme.signature_header`, and the timestamp the
  header itself carries (`{:sig, field}` schemes; `nil` otherwise).
  """
  @spec candidates(Envelope.t(), struct()) ::
          {:ok, [String.t()], String.t() | nil}
          | {:error, :missing_signature | :malformed_signature}
  def candidates(env, scheme) do
    case Envelope.header(env, scheme.signature_header) do
      nil ->
        {:error, :missing_signature}

      header ->
        case scheme.parse do
          :whole -> whole(header, scheme.sig_prefix)
          :csv_pairs -> csv_pairs(header, scheme)
          :space_versions -> space_versions(header, scheme)
        end
    end
  end

  defp whole(header, nil), do: {:ok, [header], nil}

  defp whole(header, prefix) do
    if String.starts_with?(header, prefix) do
      {:ok, [String.replace_prefix(header, prefix, "")], nil}
    else
      {:error, :malformed_signature}
    end
  end

  defp csv_pairs(header, %{sig_key: sig_key, timestamp: timestamp}) do
    parts =
      header
      |> String.split(",", trim: true)
      |> Enum.map(&String.split(&1, "=", parts: 2))

    sigs = for [^sig_key, v] <- parts, do: v

    sig_ts =
      case timestamp do
        {:sig, field} ->
          Enum.find_value(parts, fn
            [^field, v] -> v
            _ -> nil
          end)

        _ ->
          nil
      end

    if sigs != [] do
      {:ok, sigs, sig_ts}
    else
      {:error, :malformed_signature}
    end
  end

  defp space_versions(header, %{version: version}) do
    sigs =
      header
      |> String.split(" ", trim: true)
      |> Enum.flat_map(fn token ->
        case version do
          nil ->
            [token]

          v ->
            if String.starts_with?(token, v) do
              [String.replace_prefix(token, v, "")]
            else
              []
            end
        end
      end)

    if sigs != [] do
      {:ok, sigs, nil}
    else
      {:error, :malformed_signature}
    end
  end

  @doc """
  The timestamp a scheme signs, checked against the replay window in `opts`
  (`Ankusa.Verifier.check_timestamp/2`). `{:ok, nil}` for a scheme with no
  timestamp. `sig_ts` is what `candidates/2` found in the signature header.
  """
  @spec timestamp(Envelope.t(), struct(), String.t() | nil, keyword()) ::
          {:ok, String.t() | nil} | {:error, atom()}
  def timestamp(_env, %{timestamp: nil}, _sig_ts, _opts), do: {:ok, nil}

  def timestamp(env, %{timestamp: {:header, name}}, _sig_ts, opts) do
    case Envelope.header(env, name) do
      nil -> {:error, :missing_signature}
      "" -> {:error, :missing_signature}
      ts -> check_timestamp(ts, opts)
    end
  end

  def timestamp(_env, %{timestamp: {:sig, _field}}, sig_ts, opts) do
    case sig_ts do
      nil -> {:error, :missing_signature}
      ts -> check_timestamp(ts, opts)
    end
  end

  defp check_timestamp(ts, opts) do
    case Verifier.check_timestamp(ts, opts) do
      :ok -> {:ok, ts}
      {:error, _} = err -> err
    end
  end

  @doc """
  The bytes the provider signed: `scheme.signed` with `{body}`, `{ts}` and
  `{header:NAME}` filled in. A header or `{ts}` the template names but the
  request lacks is `:missing_signature`.
  """
  @spec signed_bytes(Envelope.t(), struct(), String.t() | nil) ::
          {:ok, binary()} | {:error, :missing_signature}
  def signed_bytes(env, %{signed: template}, ts) do
    with :ok <- header_values?(env, template),
         :ok <- ts_value?(template, ts) do
      {:ok,
       Regex.replace(~r/\{(body|ts|header:[^}]*)\}/, template, fn _, token ->
         case token do
           "body" -> env.body
           "ts" -> ts
           "header:" <> name -> Envelope.header(env, name)
         end
       end)}
    end
  end

  defp header_values?(env, template) do
    ~r/\{header:([^}]*)\}/
    |> Regex.scan(template, capture: :all_but_first)
    |> Enum.reduce_while(:ok, fn [name], :ok ->
      case Envelope.header(env, name) do
        nil -> {:halt, {:error, :missing_signature}}
        _ -> {:cont, :ok}
      end
    end)
  end

  defp ts_value?(template, ts) do
    if is_nil(ts) and String.contains?(template, "{ts}") do
      {:error, :missing_signature}
    else
      :ok
    end
  end
end
