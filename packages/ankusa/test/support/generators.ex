defmodule Ankusa.Generators do
  @moduledoc """
  StreamData generators for the property tests in `test/ankusa/property/`: the
  claim-check refs and packs, envelopes, header lists, CIDR strings, signature
  schemes and the 0.3 on-disk frames that the parsers and codecs take.

  Every generator mixes well-formed values with hostile ones (raw bytes that
  are not UTF-8, off-by-one boundaries, one bit flipped in a valid value), so
  a property finds the input a parser mishandles rather than only the ones its
  author thought of.
  """

  use ExUnitProperties

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.{Envelope, Verification}
  alias Ankusa.Verifier.Hmac.Scheme

  # The largest timestamp `Ankusa.ULID` decodes (9999-12-31T23:59:59.999Z).
  @max_ms 253_402_300_799_999

  # ── bytes ────────────────────────────────────────────────────────────────

  @doc "Arbitrary bytes, mostly short, sometimes a few KiB."
  def bytes(max \\ 2048) do
    frequency([
      {6, binary(max_length: 32)},
      {3, binary(max_length: 256)},
      {1, binary(max_length: max)}
    ])
  end

  @doc "A short ASCII token, visible characters only."
  def token(max \\ 24), do: string([?!..?z], min_length: 1, max_length: max)

  @doc """
  An arbitrary term of modest size. `StreamData.term/0` grows floats and
  nested containers without bound, which is too slow for thousands of runs.
  """
  def simple_term do
    leaf =
      one_of([
        integer(),
        boolean(),
        constant(nil),
        atom(:alphanumeric),
        binary(max_length: 24)
      ])

    one_of([
      leaf,
      list_of(leaf, max_length: 4),
      tuple({leaf, leaf}),
      map_of(one_of([atom(:alphanumeric), binary(max_length: 8)]), leaf, max_length: 3)
    ])
  end

  @doc """
  `bytes` with one mutation: truncated, one bit flipped, a run of bytes
  inserted, a run zeroed, or a random tail appended.
  """
  def mutated(bytes_gen) do
    bind(bytes_gen, fn bytes ->
      bind(tuple({integer(0..7), integer(0..4095), binary(max_length: 16)}), fn {bit, at, extra} ->
        size = byte_size(bytes)
        pos = if size == 0, do: 0, else: rem(at, size)

        member_of([
          binary_part(bytes, 0, pos),
          flip(bytes, pos, bit),
          binary_part(bytes, 0, pos) <> extra <> binary_part(bytes, pos, size - pos),
          zero(bytes, pos, byte_size(extra) + 1),
          bytes <> extra
        ])
      end)
    end)
  end

  defp flip(<<>>, _pos, _bit), do: <<>>

  defp flip(bytes, pos, bit) do
    <<head::binary-size(^pos), byte, tail::binary>> = bytes
    <<head::binary, Bitwise.bxor(byte, Bitwise.bsl(1, bit)), tail::binary>>
  end

  defp zero(bytes, pos, count) do
    count = min(count, byte_size(bytes) - pos)
    <<head::binary-size(^pos), _::binary-size(^count), tail::binary>> = bytes
    <<head::binary, 0::size(count * 8), tail::binary>>
  end

  # ── claim-check refs and packs ───────────────────────────────────────────

  @doc "A valid tenant name: `[A-Za-z0-9_-]{1,64}`."
  def tenant_id do
    string([?a..?z, ?A..?Z, ?0..?9, ?_, ?-], min_length: 1, max_length: 64)
  end

  @doc "A pack id: 48-bit time (clamped to what `DateTime` holds), 64 random bits, 16 zero bits."
  def pack_id do
    gen all ms <- one_of([integer(0..@max_ms), member_of([0, 1, @max_ms])]),
            entropy <- binary(length: 8) do
      Ref.pack_id(ms, entropy)
    end
  end

  @doc "A position in a pack, with both ends of the 16-bit range over-represented."
  def pack_index do
    frequency([{4, integer(0..0xFFFF)}, {1, member_of([0, 1, 0xFFFE, 0xFFFF])}])
  end

  @doc "A claim id: a pack id with a position."
  def claim_id do
    gen all pack <- pack_id(), index <- pack_index() do
      Ref.claim_id(pack, index)
    end
  end

  @doc "A valid `%Ankusa.ClaimCheck.Ref{}`."
  def ref do
    gen all tenant <- tenant_id(), claim <- claim_id() do
      %Ref{tenant_id: tenant, claim_id: claim}
    end
  end

  @doc "A claim body of 0 bytes to 1 MiB: mostly short, sometimes tens of KiB, rarely large."
  def claim_body do
    frequency([
      {10, binary(max_length: 64)},
      {3, binary(max_length: 1024)},
      {2, repeated(65_536)},
      {1, repeated(1_048_576)}
    ])
  end

  # StreamData builds a binary byte by byte, which is too slow by the KiB: a
  # random 16-byte seed repeated to a random length tests byte-exact placement
  # as well as random bytes do.
  defp repeated(max) do
    map({integer(0..max), binary(length: 16)}, fn {size, seed} ->
      binary_part(:binary.copy(seed, div(size, 16) + 1), 0, size)
    end)
  end

  @doc """
  `claims` for `Ankusa.ClaimCheck.Pack.build/1`: one pack id, claim ids at
  positions `0..n-1`, bodies within a ~2 MiB budget, and the digest of each
  body.
  """
  def pack_claims do
    gen all pack <- pack_id(),
            count <- frequency([{8, integer(1..8)}, {3, integer(9..64)}, {1, integer(65..600)}]),
            bodies <- list_of(claim_body(), length: count),
            ids <- list_of(uuid(), length: count),
            types <- list_of(one_of([constant(nil), token()]), length: count),
            times <-
              list_of(one_of([constant(nil), integer(0..4_102_444_800_000)]), length: count) do
      bodies = budget(bodies, 2_097_152)

      [bodies, ids, types, times]
      |> Enum.zip()
      |> Enum.with_index()
      |> Enum.map(fn {{body, id, type, at}, index} ->
        %{
          claim_id: Ref.claim_id(pack, index),
          id: id,
          body: body,
          sha256: Base.encode16(:crypto.hash(:sha256, body), case: :lower),
          content_type: type,
          received_at: at
        }
      end)
    end
  end

  # Past the budget a body shrinks to empty: the count stays, the total does not.
  defp budget(bodies, limit) do
    {kept, _} =
      Enum.map_reduce(bodies, 0, fn body, used ->
        if used + byte_size(body) <= limit, do: {body, used + byte_size(body)}, else: {<<>>, used}
      end)

    kept
  end

  @doc "A UUID-shaped string."
  def uuid do
    gen all raw <- binary(length: 16) do
      <<a::binary-4, b::binary-2, c::binary-2, d::binary-2, e::binary-6>> = raw

      [a, b, c, d, e]
      |> Enum.map_join("-", &Base.encode16(&1, case: :lower))
    end
  end

  # ── envelopes ────────────────────────────────────────────────────────────

  @doc "Header names and values as raw bytes: nothing in them is guaranteed valid."
  def raw_header do
    tuple({bytes(24), bytes(64)})
  end

  @doc """
  Header lists as a listener could hand over: valid ASCII pairs, pairs with
  control bytes, tabs and spaces in the right and wrong places, and raw bytes.
  """
  def headers(max \\ 8) do
    list_of(
      frequency([
        {5, tuple({token(), string([0x20..0x7E, ?\t], max_length: 48)})},
        {2, tuple({token(), bytes(48)})},
        {1, tuple({bytes(16), bytes(16)})},
        {1, tuple({token(), member_of(["a\r\nb", "a\nb", <<0>>, "é", <<0x7F>>, <<0xFF>>])})}
      ]),
      max_length: max
    )
  end

  @doc "A `%Ankusa.Verification{}` of any shape the verifier produces."
  def verification do
    gen all status <- member_of([:ok, :failed, :skipped]),
            provider <- member_of([nil, Ankusa.Verifier.Hmac, Ankusa.Verifier.None]),
            scheme <- one_of([constant(nil), token()]),
            reason <- one_of([constant(nil), member_of([:no_match, :bad_secret]), token()]),
            flagged <- boolean() do
      %Verification{
        status: status,
        provider: provider,
        scheme: scheme,
        reason: reason,
        flagged: flagged
      }
    end
  end

  @doc "A fully populated `%Ankusa.Envelope{}`."
  def envelope do
    gen all id <- uuid(),
            source_id <- token(),
            tenant_id <- one_of([constant(nil), tenant_id()]),
            received_at <- integer(0..4_102_444_800_000),
            method <- member_of(["POST", "PUT", "PATCH", "GET"]),
            path <- string(:printable, max_length: 80),
            headers <- list_of(raw_header(), max_length: 8),
            content_type <- one_of([constant(nil), token()]),
            body <- bytes(4096),
            seq <- one_of([constant(nil), integer(0..0xFFFFFFFFFFFFFFFF)]),
            verification <- one_of([constant(nil), verification()]),
            dedupe_key <- one_of([constant(nil), token(200)]) do
      %Envelope{
        id: id,
        source_id: source_id,
        tenant_id: tenant_id,
        received_at: received_at,
        method: method,
        path: path,
        headers: headers,
        content_type: content_type,
        body: body,
        seq: seq,
        verification: verification,
        dedupe_key: dedupe_key,
        size: byte_size(body)
      }
    end
  end

  # ── CIDRs ────────────────────────────────────────────────────────────────

  @doc "An IPv4 address as a tuple."
  def ipv4, do: tuple({integer(0..255), integer(0..255), integer(0..255), integer(0..255)})

  @doc "An IPv6 address as a tuple, with runs of zeros and the mapped prefix over-represented."
  def ipv6 do
    group = frequency([{3, integer(0..0xFFFF)}, {2, constant(0)}, {1, constant(0xFFFF)}])

    frequency([
      {4, tuple({group, group, group, group, group, group, group, group})},
      {1, map(ipv4(), fn {a, b, c, d} -> {0, 0, 0, 0, 0, 0xFFFF, a * 256 + b, c * 256 + d} end)}
    ])
  end

  @doc """
  CIDR strings: well-formed ones (`{string, ip, prefix}` is built by
  `cidr_parts/0`), and the strings a typo or an attacker makes of them.
  """
  def cidr_string do
    frequency([
      {4, map(cidr_parts(), fn {string, _ip, _prefix} -> string end)},
      {2, junk_cidr()},
      {2, bytes(40)},
      {1, string(:printable, max_length: 40)}
    ])
  end

  @doc "`{text, ip_tuple, prefix | nil}` for a well-formed CIDR (`nil` prefix: a bare address)."
  def cidr_parts do
    gen all ip <- one_of([ipv4(), ipv6()]),
            prefix <- one_of([constant(nil), integer(0..128)]) do
      text = ip |> :inet.ntoa() |> to_string()
      max = if tuple_size(ip) == 4, do: 32, else: 128

      case prefix do
        nil -> {text, ip, nil}
        p -> {"#{text}/#{rem(p, max + 1)}", ip, rem(p, max + 1)}
      end
    end
  end

  defp junk_cidr do
    gen all ip <- one_of([ipv4(), ipv6()]),
            suffix <-
              member_of([
                "/",
                "//",
                "/ 8",
                "/-1",
                "/+8",
                "/8/8",
                "/33",
                "/129",
                "/999999999999999999999",
                "/x",
                "%eth0",
                "/\u00e9",
                <<0xFF>>,
                " ",
                "\n"
              ]),
            at <- member_of([:suffix, :prefix, :only]) do
      text = ip |> :inet.ntoa() |> to_string()

      case at do
        :suffix -> text <> suffix
        :prefix -> suffix <> text
        :only -> suffix
      end
    end
  end

  # ── verifier schemes ─────────────────────────────────────────────────────

  @doc """
  A template as a list of pieces: `{:lit, binary}` (no braces),
  `:body`, `:ts`, `{:header, name}`. `render/1` turns it into the
  `signed` string of a `%Scheme{}`; `expand/2` gives the bytes it signs.
  """
  def template_pieces do
    piece =
      frequency([
        {4, map(bytes(12), fn b -> {:lit, strip_braces(b)} end)},
        {3, constant(:body)},
        {2, constant(:ts)},
        {2, map(string([?a..?z, ?0..?9, ?-], min_length: 1, max_length: 12), &{:header, &1})}
      ])

    list_of(piece, min_length: 1, max_length: 6)
  end

  defp strip_braces(bytes), do: for(<<b <- bytes>>, b not in [?{, ?}], into: <<>>, do: <<b>>)

  @doc "The `signed` template string for `pieces`."
  def render(pieces) do
    Enum.map_join(pieces, fn
      {:lit, text} -> text
      :body -> "{body}"
      :ts -> "{ts}"
      {:header, name} -> "{header:#{name}}"
    end)
  end

  @doc "The bytes `pieces` stand for, given the body, the timestamp and the request's headers."
  def expand(pieces, body, ts, headers) do
    Enum.map_join(pieces, fn
      {:lit, text} -> text
      :body -> body
      :ts -> ts
      {:header, name} -> Map.fetch!(headers, name)
    end)
  end

  @doc "Templates and headers a hostile sender or a careless operator could produce."
  def raw_template do
    frequency([
      {3, map(template_pieces(), &render/1)},
      {2, bytes(64)},
      {2,
       map(
         list_of(
           member_of([
             "{",
             "}",
             "{body",
             "body}",
             "{ts}",
             "{body}",
             "{header:",
             "{header:a}",
             "{x}",
             "{}",
             "."
           ]),
           max_length: 8
         ),
         &Enum.join/1
       )}
    ])
  end

  @doc "A syntactically valid `%Scheme{}` carrying `signed` (template pieces) and its own timestamp source."
  def scheme(pieces) do
    needs_ts? = :ts in pieces

    gen all parse <- member_of([:whole, :csv_pairs, :space_versions]),
            hash <- member_of([:sha256, :sha512, :sha1]),
            encoding <- member_of([:hex, :base64]),
            prefix <- one_of([constant(nil), member_of(["sha256=", "v0=", "sig:"])]),
            version <- one_of([constant(nil), member_of(["v1,", "v1=", "s:"])]),
            sig_key <- member_of(["v1", "sig"]),
            ts_from <- member_of([:header, :sig]) do
      timestamp =
        cond do
          not needs_ts? -> nil
          parse == :csv_pairs and ts_from == :sig -> {:sig, "t"}
          true -> {:header, "x-ts"}
        end

      %Scheme{
        signature_header: "x-signature",
        parse: parse,
        sig_prefix: if(parse == :whole, do: prefix),
        sig_key: if(parse == :csv_pairs, do: sig_key),
        version: if(parse == :space_versions, do: version),
        signed: render(pieces),
        hash: hash,
        encoding: encoding,
        timestamp: timestamp
      }
    end
  end

  @doc "An arbitrary `%Scheme{}`: any field may be wrong, so `validate!/1` has work to do."
  def hostile_scheme do
    gen all header <- one_of([token(), constant(""), constant(nil), constant(5)]),
            parse <- member_of([:whole, :csv_pairs, :space_versions, :other, nil]),
            sig_prefix <- one_of([constant(nil), bytes(8)]),
            sig_key <- one_of([constant(nil), token(6)]),
            version <- one_of([constant(nil), bytes(8)]),
            signed <- raw_template(),
            hash <- member_of([:sha256, :sha512, :sha1, :md5, nil]),
            encoding <- member_of([:hex, :base64, :base32, nil]),
            secret_decode <- member_of([:raw, :whsec_base64, :other]),
            timestamp <-
              one_of([
                constant(nil),
                map(token(), &{:header, &1}),
                map(token(), &{:sig, &1}),
                constant({:header, ""}),
                constant(:bad)
              ]) do
      %Scheme{
        signature_header: header,
        parse: parse,
        sig_prefix: sig_prefix,
        sig_key: sig_key,
        version: version,
        signed: signed,
        hash: hash,
        encoding: encoding,
        secret_decode: secret_decode,
        timestamp: timestamp
      }
    end
  end

  # ── dedupe ───────────────────────────────────────────────────────────────

  @doc "A JSON-encodable value that is not a string or an integer (a dedupe key yields `nil` for it)."
  def other_json do
    one_of([
      constant(nil),
      boolean(),
      float(),
      list_of(integer(), max_length: 3),
      map_of(token(4), integer(), max_length: 3)
    ])
  end

  # ── legacy 0.3 files ─────────────────────────────────────────────────────

  @doc "A legacy WAL frame around `payload`, with a CRC that matches it."
  def wal_frame(seq, payload) do
    crc = :erlang.crc32(payload)
    <<0x484B::16, 1::8, 0::8, seq::64, crc::32, byte_size(payload)::32, payload::binary>>
  end

  @doc "A legacy WAL frame for a generated envelope with `seq`."
  def wal_frame_for(seq, %Envelope{} = env) do
    wal_frame(seq, :erlang.term_to_binary(Map.from_struct(%{env | seq: seq}), [:deterministic]))
  end

  @doc "A legacy term-log frame: `<<len::32, term_to_binary(term)>>`."
  def log_frame(term) do
    bin = :erlang.term_to_binary(term)
    <<byte_size(bin)::32, bin::binary>>
  end
end
