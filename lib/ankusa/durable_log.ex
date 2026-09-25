defmodule Ankusa.DurableLog do
  @moduledoc """
  Append-only log of `:erlang.term_to_binary/1` records, each framed by a 32-bit
  length prefix.

  Several components keep local durable state this way — `Ankusa.Dispatch.DLQ`,
  `Ankusa.Edge.Quarantine`, `Ankusa.Storage.Index` — and all of them need the
  same three properties, which is why this exists once instead of three times:

    * **an append is one write.** `append/2` frames every record and issues a
      single :file.write/3 call with `[:append]`, so a crash mid-batch can only
      damage the last frame.
    * **a torn tail is dropped, never raised on.** `read/2` decodes frames until
      one is short — a write that never completed — and stops there. A record
      that wasn't fully written was never acknowledged, so dropping it is the
      honest behaviour, and it is the same discipline the WAL applies to its own
      replay.
    * **atom safety is explicit.** See below.

  ## On-disk format

      <<len::32, term::binary>>

  No CRC: `term_to_binary/1` is self-delimiting and the length prefix is what
  detects a torn append. `Ankusa.Codec.Raw` frames *segment payloads* with a CRC
  instead, and `Ankusa.WAL.DiskLog` adds a magic/version/seq header to its own
  frames — both have needs (integrity of attacker-supplied bytes; offset
  addressing) this log doesn't.

  ## Atom safety

  `read/2` decodes with `:erlang.binary_to_term/2` and the `:safe` option by
  default, which refuses to intern atoms a file asks for. Records that
  legitimately carry *pre-existing* atoms must pass `safe: false`: an
  `Ankusa.Envelope` holds verifier module names, and after a restart those atoms
  may not be interned yet, so `:safe` would reject valid, self-written data —
  the reasoning `Ankusa.Envelope.from_binary/1` documents. Logs holding only
  strings and numbers (the storage index) keep the `:safe` default.
  """

  @type opts :: [safe: boolean()]
  @type append_opts :: [sync: boolean()]

  @doc """
  Append one record, or several in a single write.

  Creates the parent directory if needed. The frames are flushed by the OS
  unless `sync: true`, which adds an `fsync` before returning — callers that
  acknowledge on the strength of the append (the DLQ, the storage index) pass
  it, because a write that is only in the page cache is a write a power loss
  can drop. `Ankusa.Edge.Quarantine` keeps its own descriptor and always
  `:file.datasync/1`s its puts.
  """
  @spec append(Path.t(), term() | [term()], append_opts()) :: :ok
  def append(path, records, opts \\ [])

  def append(path, records, opts) when is_list(records) do
    if Keyword.get(opts, :sync, false) do
      append_synced(path, records)
    else
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, frame(records), [:append, :binary])
      :ok
    end
  end

  def append(path, record, opts), do: append(path, [record], opts)

  defp append_synced(path, records) do
    File.mkdir_p!(Path.dirname(path))
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])

    try do
      :ok = :file.write(fd, frame(records))
      :ok = :file.datasync(fd)
    after
      :file.close(fd)
    end

    :ok
  end

  @doc """
  Frame records as iodata for a single write.

  For callers that hold their own file descriptor and decide their own
  durability barrier — `Ankusa.Edge.Quarantine` keeps one open and
  `:file.datasync/1`s each put — while still sharing this one definition of the
  on-disk format.
  """
  @spec frame(term() | [term()]) :: iodata()
  def frame(records) when is_list(records), do: Enum.map(records, &frame_record/1)
  def frame(record), do: frame([record])

  @doc """
  Read every complete record in order. A missing file is an empty log; a torn
  trailing record is dropped.
  """
  @spec read(Path.t(), opts()) :: [term()]
  def read(path, opts \\ []) do
    case File.read(path) do
      {:ok, bin} -> decode(bin, opts)
      {:error, _} -> []
    end
  end

  @doc """
  Decode a binary already in the on-disk format (what `frame/1` produces).

  Lets a caller read records that arrived over the wire — e.g. an
  `Ankusa.Storage.Index` sidecar fetched from the blob store — with the same
  framing rules, including dropping a torn trailing record, as `read/2`.
  """
  @spec decode(binary(), opts()) :: [term()]
  def decode(bin, opts \\ []), do: parse(bin, [], Keyword.get(opts, :safe, true))

  defp frame_record(record) do
    bin = :erlang.term_to_binary(record)
    [<<byte_size(bin)::32>>, bin]
  end

  defp parse(<<len::32, rest::binary>>, acc, safe) when byte_size(rest) >= len do
    <<record::binary-size(^len), tail::binary>> = rest
    parse(tail, [decode_term(record, safe) | acc], safe)
  end

  defp parse(_torn_or_empty, acc, _safe), do: Enum.reverse(acc)

  defp decode_term(record, true), do: :erlang.binary_to_term(record, [:safe])
  defp decode_term(record, false), do: :erlang.binary_to_term(record)
end
