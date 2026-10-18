defmodule Ankusa.Property.HmacTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.Envelope
  alias Ankusa.Generators, as: G
  alias Ankusa.Verifier.Hmac
  alias Ankusa.Verifier.Hmac.Scheme
  alias Ankusa.Verifier.Schemes

  @reasons [
    :missing_signature,
    :malformed_signature,
    :no_match,
    :timestamp_out_of_tolerance,
    :bad_secret,
    :bad_scheme
  ]

  @now 1_790_258_591

  defp env(headers, body) do
    %Envelope{
      id: "id",
      source_id: "src",
      received_at: 0,
      method: "POST",
      path: "/",
      headers: headers,
      body: body,
      size: byte_size(body)
    }
  end

  # A request aimed at `scheme`: junk headers, plus (usually) the header the
  # scheme reads its signature and timestamp from, holding junk of the right
  # and the wrong shape.
  defp request_for(scheme) do
    sig_header = if is_binary(scheme.signature_header), do: scheme.signature_header

    gen all junk <- list_of(G.raw_header(), max_length: 4),
            signature <-
              one_of([
                G.bytes(80),
                member_of([
                  "",
                  "t=1790258591,v1=abc",
                  "t=,v1=",
                  "v1,abc v1,def v2,ghi",
                  "sha256=abc",
                  "v0=" <> String.duplicate("0", 64),
                  "=",
                  ",,,",
                  " "
                ])
              ]),
            ts <-
              one_of([G.bytes(12), member_of([Integer.to_string(@now), "0", "-1", "9e9", "x"])]),
            present? <- frequency([{5, constant(true)}, {1, constant(false)}]),
            body <- G.bytes(256) do
      headers =
        junk ++
          if(sig_header && present?, do: [{sig_header, signature}], else: []) ++
          case scheme.timestamp do
            {:header, name} when is_binary(name) and present? -> [{name, ts}]
            _ -> []
          end

      env(headers, body)
    end
  end

  defp secrets do
    one_of([
      G.bytes(32),
      list_of(G.bytes(32), max_length: 3),
      constant(nil),
      constant(""),
      constant([]),
      constant(5),
      map(G.bytes(24), &["whsec_" <> Base.encode64(&1)])
    ])
  end

  property "validate!/1 on any scheme returns it or raises ArgumentError, nothing else" do
    check all scheme <- G.hostile_scheme() do
      try do
        assert Schemes.validate!(scheme) == scheme
      rescue
        ArgumentError -> :ok
      end
    end
  end

  property "validate_opts/1 on any scheme and secret answers :ok or the error verify/2 would" do
    check all scheme <- one_of([G.hostile_scheme(), member_of(Map.keys(Schemes.all()))]),
              secret <- secrets() do
      assert Hmac.validate_opts(scheme: scheme, secret: secret) in [
               :ok,
               {:error, :bad_scheme},
               {:error, :bad_secret}
             ]
    end
  end

  property "verify/2 never raises for any inline scheme, template, headers or secret" do
    check all scheme <- G.hostile_scheme(),
              request <- request_for(scheme),
              secret <- secrets() do
      assert_verdict(Hmac.verify(request, scheme: scheme, secret: secret, now: @now))
    end
  end

  property "verify/2 never raises for a valid scheme over arbitrary templates and requests" do
    check all pieces <- G.template_pieces(),
              scheme <- G.scheme(pieces),
              request <- request_for(scheme),
              secret <- secrets() do
      assert_verdict(Hmac.verify(request, scheme: scheme, secret: secret, now: @now))
    end
  end

  property "every preset survives arbitrary headers, bodies and secrets" do
    check all name <- member_of(Map.keys(Schemes.all())),
              request <- request_for(Map.fetch!(Schemes.all(), name)),
              secret <- secrets() do
      assert_verdict(Hmac.verify(request, scheme: name, secret: secret, now: @now))
    end
  end

  property "a request verifies with its signing key alone, and with the rotation list that holds it" do
    check all signed <- signed_request() do
      %{env: env, scheme: scheme, secrets: secrets, signer: signer} = signed

      assert Hmac.verify(env, scheme: scheme, secret: secrets, now: @now) == :ok
      assert Hmac.verify(env, scheme: scheme, secret: signer, now: @now) == :ok
      assert Hmac.verify(env, scheme: scheme, secret: Enum.reverse(secrets), now: @now) == :ok
    end
  end

  property "changing the body breaks a signature that covers it" do
    check all signed <- signed_request(), extra <- binary(min_length: 1, max_length: 8) do
      %{env: env, scheme: scheme, secrets: secrets, pieces: pieces} = signed
      tampered = %{env | body: env.body <> extra}

      if :body in pieces do
        assert Hmac.verify(tampered, scheme: scheme, secret: secrets, now: @now) ==
                 {:error, :no_match}
      else
        assert Hmac.verify(tampered, scheme: scheme, secret: secrets, now: @now) == :ok
      end
    end
  end

  property "a key that did not sign does not verify" do
    check all signed <- signed_request(), other <- binary(min_length: 1, max_length: 32) do
      %{env: env, scheme: scheme, secrets: secrets} = signed

      if other not in secrets do
        assert Hmac.verify(env, scheme: scheme, secret: other, now: @now) == {:error, :no_match}
      end
    end
  end

  property "a signed timestamp outside the window is refused, inside it is not" do
    check all signed <- signed_request(),
              skew <- one_of([integer(-300..300), integer(301..100_000)]) do
      %{env: env, scheme: scheme, secrets: secrets} = signed
      verdict = Hmac.verify(env, scheme: scheme, secret: secrets, now: @now + skew)

      cond do
        scheme.timestamp == nil -> assert verdict == :ok
        abs(skew) <= 300 -> assert verdict == :ok
        true -> assert verdict == {:error, :timestamp_out_of_tolerance}
      end
    end
  end

  # ── a correctly signed request, built without the verifier's regexes ─────

  defp signed_request do
    gen all pieces <- G.template_pieces(),
            scheme <- G.scheme(pieces),
            body <- G.bytes(512),
            secrets <-
              list_of(binary(min_length: 1, max_length: 32), min_length: 1, max_length: 3),
            signer <- integer(0..2),
            names = for({:header, name} <- pieces, do: "h-" <> name) |> Enum.uniq(),
            values <- list_of(G.bytes(20), length: length(names)),
            junk <- list_of(G.raw_header(), max_length: 3) do
      pieces =
        Enum.map(pieces, fn
          {:header, name} -> {:header, "h-" <> name}
          other -> other
        end)

      scheme = %{scheme | signed: G.render(pieces)}

      values = Map.new(Enum.zip(names, values))
      ts = Integer.to_string(@now)
      secret = Enum.at(secrets, rem(signer, length(secrets)))
      hash = if scheme.hash == :sha1, do: :sha, else: scheme.hash
      mac = :crypto.mac(:hmac, hash, secret, G.expand(pieces, body, ts, values))

      sig =
        if scheme.encoding == :hex, do: Base.encode16(mac, case: :lower), else: Base.encode64(mac)

      {sig_header, ts_header} = signature_headers(scheme, sig, ts)

      headers =
        Enum.reject(junk, fn {name, _} ->
          String.downcase(name) in ["x-signature", "x-ts" | names]
        end) ++
          Enum.map(values, fn {name, value} -> {name, value} end) ++
          [{"x-signature", sig_header}] ++ ts_header

      %{env: env(headers, body), scheme: scheme, secrets: secrets, signer: secret, pieces: pieces}
    end
  end

  defp signature_headers(%Scheme{parse: :whole, sig_prefix: prefix} = scheme, sig, ts) do
    {(prefix || "") <> sig, timestamp_header(scheme, ts)}
  end

  defp signature_headers(
         %Scheme{parse: :csv_pairs, sig_key: key, timestamp: timestamp} = scheme,
         sig,
         ts
       ) do
    parts =
      [if(match?({:sig, _}, timestamp), do: "t=#{ts}"), "v0=decoy", "#{key}=#{sig}"]
      |> Enum.reject(&is_nil/1)

    {Enum.join(parts, ","), timestamp_header(scheme, ts)}
  end

  defp signature_headers(%Scheme{parse: :space_versions, version: version} = scheme, sig, ts) do
    tokens =
      case version do
        nil -> ["decoy", sig]
        v -> [v <> "decoy", v <> sig]
      end

    {Enum.join(tokens, " "), timestamp_header(scheme, ts)}
  end

  defp timestamp_header(%Scheme{timestamp: {:header, name}}, ts), do: [{name, ts}]
  defp timestamp_header(_scheme, _ts), do: []

  defp assert_verdict(:ok), do: :ok
  defp assert_verdict({:error, reason}), do: assert(reason in @reasons)
end
