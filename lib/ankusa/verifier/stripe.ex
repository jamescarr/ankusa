defmodule Ankusa.Verifier.Stripe do
  @moduledoc ~S"""
  Verifier for Stripe webhook signatures.

  The `Stripe-Signature` header is `t=<unix>,v1=<hexsig>[,v1=...]`. The secret is
  the raw `whsec_...` string used verbatim as the HMAC key (Stripe does *not*
  Base64-decode it). Signed content is `"#{t}.#{body}"`, HMAC-SHA256, lowercase
  hex. Any `v1` value that matches accepts (constant-time).
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier

  @default_tolerance 300

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    header = Envelope.header(env, "Stripe-Signature")

    with true <- is_binary(header),
         %{"t" => t, v1: sigs} <- parse(header),
         :ok <- check_timestamp(t, opts) do
      secret = Keyword.get(opts, :secret, "")
      signed = "#{t}.#{env.body}"
      expected = Base.encode16(:crypto.mac(:hmac, :sha256, secret, signed), case: :lower)

      if Enum.any?(sigs, &Verifier.secure_compare(&1, expected)) do
        :ok
      else
        {:error, :no_match}
      end
    else
      false -> {:error, :missing_signature}
      {:error, _} = err -> err
      _ -> {:error, :malformed_signature}
    end
  end

  defp parse(header) do
    parts =
      header
      |> String.split(",", trim: true)
      |> Enum.map(&String.split(&1, "=", parts: 2))

    t =
      Enum.find_value(parts, fn
        ["t", v] -> v
        _ -> nil
      end)

    sigs =
      for ["v1", v] <- parts, do: v

    if t && sigs != [] do
      %{"t" => t, v1: sigs}
    else
      {:error, :malformed_signature}
    end
  end

  defp check_timestamp(t, opts) do
    tolerance = Keyword.get(opts, :tolerance, @default_tolerance)

    case Integer.parse(t) do
      {t_int, _} ->
        now = System.system_time(:second)

        if abs(now - t_int) <= tolerance do
          :ok
        else
          {:error, :timestamp_out_of_tolerance}
        end

      :error ->
        {:error, :malformed_signature}
    end
  end
end
