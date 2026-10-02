defmodule Ankusa.Test.LegacyDataDir do
  @moduledoc """
  Writes the bytes a 0.3 node leaves on disk, without the modules that wrote
  them (`Ankusa.WAL.DiskLog`, `Ankusa.DurableLog`, ... no longer exist). The
  formats are pinned here on purpose: `Ankusa.Store.Migrate` has to read what
  real 0.3 nodes wrote, not what today's code would write.

    * WAL frame: `<<0x484B::16, 1::8, 0::8, seq::64, crc32::32, len::32, payload>>`,
      `payload` = the envelope as `:erlang.term_to_binary/2` (`:deterministic`) with
      its `seq` set; `crc32` covers the payload.
    * `ankusa.wal.cursors` = `term_to_binary(%{dispatch: n, compactor: m})`,
      `ankusa.wal.truncated` = `term_to_binary(floor)`.
    * term logs (DLQ, quarantine, segment index): `<<byte_size(bin)::32, bin>>`
      frames of `term_to_binary(record)`.
  """

  alias Ankusa.{Codec, Config, Envelope}

  @doc """
  Write `wal/ankusa.wal` with one frame per envelope (each must carry its `seq`)
  and the two sidecars.

  Options: `cursors:` (a map, default `%{}`: no file), `floor:` (an integer,
  default none).
  """
  def wal!(config, envs, opts \\ []) do
    dir = Config.path(config, "wal")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "ankusa.wal"), Enum.map(envs, &wal_frame/1))

    case Keyword.get(opts, :cursors) do
      nil ->
        :ok

      cursors ->
        File.write!(Path.join(dir, "ankusa.wal.cursors"), :erlang.term_to_binary(cursors))
    end

    case Keyword.get(opts, :floor) do
      nil -> :ok
      floor -> File.write!(Path.join(dir, "ankusa.wal.truncated"), :erlang.term_to_binary(floor))
    end

    Path.join(dir, "ankusa.wal")
  end

  @doc "One framed WAL record, as iodata."
  def wal_frame(%Envelope{seq: seq} = env) when is_integer(seq) do
    payload = :erlang.term_to_binary(Map.from_struct(env), [:deterministic])
    crc = :erlang.crc32(payload)
    <<0x484B::16, 1::8, 0::8, seq::64, crc::32, byte_size(payload)::32, payload::binary>>
  end

  @doc "Byte offset at which each frame of a WAL file starts."
  def frame_offsets(path) do
    walk(File.read!(path), 0, [])
  end

  defp walk(<<>>, _pos, acc), do: Enum.reverse(acc)

  defp walk(
         <<0x484B::16, 1::8, _::8, _seq::64, _crc::32, len::32, _::binary-size(len),
           rest::binary>>,
         pos,
         acc
       ) do
    walk(rest, pos + 20 + len, [pos | acc])
  end

  defp walk(_torn, _pos, acc), do: Enum.reverse(acc)

  @doc "Write a term log (`dlq/dlq.log`, `quarantine/quarantine.log`, `segments/index.log`)."
  def log!(path, terms) do
    File.mkdir_p!(Path.dirname(path))

    File.write!(
      path,
      Enum.map(terms, fn term ->
        bin = :erlang.term_to_binary(term)
        <<byte_size(bin)::32, bin::binary>>
      end)
    )

    path
  end

  @doc "A 0.3 DLQ entry."
  def dlq_entry(%Envelope{} = env, reason, at), do: %{envelope: env, reason: reason, at: at}

  @doc """
  Write `segments/<key>` as `Ankusa.Codec.Raw` did and return its 0.3 index rows,
  ready for `log!(Config.path(config, "segments/index.log"), rows)`.
  """
  def segment!(config, key, envs) do
    records = Enum.map(envs, fn env -> %{key: env.id, payload: Envelope.to_binary(env)} end)
    {segment, index} = Codec.Raw.encode(records)
    path = Path.join(Config.path(config, "segments"), key)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, segment)

    envs
    |> Enum.zip(index)
    |> Enum.map(fn {env, entry} ->
      %{
        event_id: env.id,
        source_id: env.source_id,
        tenant_id: env.tenant_id,
        received_at: env.received_at,
        seq: env.seq,
        segment_key: key,
        offset: entry.offset,
        length: entry.length
      }
    end)
  end

  def sources_json!(config, entries) do
    body = %{
      "version" => 1,
      "sources" =>
        Enum.map(entries, fn {tenant, name, spec} ->
          %{"tenant" => tenant, "name" => name, "spec" => spec}
        end)
    }

    write_json!(Config.path(config, "sources.json"), body)
  end

  def rate_limits_json!(config, tenants) do
    body = %{
      "version" => 1,
      "tenants" =>
        Map.new(tenants, fn {tenant, rate, burst} ->
          {tenant, %{"rate" => rate, "burst" => burst}}
        end)
    }

    write_json!(Config.path(config, "rate_limits.json"), body)
  end

  defp write_json!(path, body) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode!(body))
    path
  end

  @doc "Flip one bit of the byte at `offset`."
  def flip_byte!(path, offset) do
    bin = File.read!(path)
    <<head::binary-size(^offset), byte, tail::binary>> = bin
    File.write!(path, <<head::binary, Bitwise.bxor(byte, 1), tail::binary>>)
  end
end
