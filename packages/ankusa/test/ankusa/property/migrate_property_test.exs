defmodule Ankusa.Property.MigrateTest do
  @moduledoc """
  The 0.3 import reads files from disk that may be torn, bit-flipped or not
  what 0.3 wrote. Its frame scanner has to give one of two answers, never a
  third: the import ran (`:ok`, a torn final frame forgiven), or the boot is
  refused with a named error because acked data sits beyond the damage.

  One store per test, one data dir reset between runs: what is under test is
  the scanner and its decisions, not RocksDB.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Store}
  alias Ankusa.Generators, as: G
  alias Ankusa.Store.{Keys, Migrate}

  @moduletag capture_log: true

  @artifacts ~w(sources rate_limits quarantine wal dlq index)
  @header 20

  setup do
    config = test_config(roles: [:edge, :dispatch, :storage], storage: %{interval_ms: 0})
    put_config(config)
    start_supervised!({Store, instance: config.instance})
    %{config: config}
  end

  # Everything a previous run left: the artifacts, what they were renamed to,
  # and the markers that say they were imported. The store itself stays.
  defp reset!(config) do
    dir = Config.path(config, "")

    for entry <- File.ls!(dir), entry != "store", do: File.rm_rf!(Path.join(dir, entry))

    deletes =
      for key <- ["migration" | Enum.map(@artifacts, &("imported:" <> &1))],
          do: {:delete, :default, Keys.meta(key)}

    :ok = Store.write(config.instance, deletes)
  end

  defp wal_path(config), do: Path.join(Config.path(config, "wal"), "ankusa.wal")

  defp put_wal!(config, bytes) do
    File.mkdir_p!(Path.dirname(wal_path(config)))
    File.write!(wal_path(config), bytes)
  end

  defp hooks_present(config, seqs) do
    for seq <- seqs,
        match?({:ok, _}, Store.get(config.instance, :hooks, Keys.hook(seq))),
        do: seq
  end

  defp forget_hooks!(config, seqs) do
    :ok = Store.write(config.instance, for(seq <- seqs, do: {:delete, :hooks, Keys.hook(seq)}))
  end

  # n frames for seqs 1..n, and the byte offset each starts at.
  defp frames(envs) do
    envs
    |> Enum.with_index(1)
    |> Enum.map(fn {env, seq} -> IO.iodata_to_binary(G.wal_frame_for(seq, env)) end)
  end

  defp offsets(frames) do
    {offsets, _} = Enum.map_reduce(frames, 0, fn frame, at -> {at, at + byte_size(frame)} end)
    offsets
  end

  defp wal_envs(min \\ 1), do: list_of(G.envelope(), min_length: min, max_length: 5)

  # The torn tail 0.3 left behind: a frame cut anywhere, or a header whose length
  # claims far more bytes than the file holds (never read at that length).
  property "a torn tail is forgiven: every whole frame imports, the torn one does not", %{
    config: config
  } do
    check all envs <- wal_envs(),
              cut_at <- integer(0..100_000),
              tail <-
                one_of([
                  constant(:cut),
                  map(
                    {member_of([0xFFFFFFFF, 0x10000000, 0x0FFFFFFF, 0x7FFFFFFF]), G.bytes(64)},
                    fn {claimed, junk} ->
                      {:claims, <<0x484B::16, 1::8, 0::8, 99::64, 0::32, claimed::32>> <> junk}
                    end
                  )
                ]) do
      reset!(config)
      frames = frames(envs)
      bytes = IO.iodata_to_binary(frames)
      ends = Enum.map(Enum.zip(offsets(frames), frames), fn {at, f} -> at + byte_size(f) end)

      {file, whole} =
        case tail do
          :cut ->
            cut = rem(cut_at, byte_size(bytes) + 1)
            {binary_part(bytes, 0, cut), Enum.count(ends, &(&1 <= cut))}

          {:claims, header} ->
            {bytes <> header, length(envs)}
        end

      forget_hooks!(config, 1..length(envs))
      put_wal!(config, file)

      assert Migrate.run(config) == :ok
      assert hooks_present(config, 1..length(envs)) == Enum.to_list(1..whole//1)
    end
  end

  property "damage with an acked frame after it refuses the boot and imports nothing", %{
    config: config
  } do
    check all envs <- wal_envs(2),
              victim <- integer(0..3),
              damage <- member_of([:payload, :crc, :length, :magic, :version, :zeroed]),
              bit <- integer(0..7),
              at <- integer(0..4095) do
      reset!(config)
      frames = frames(envs)
      victim = rem(victim, length(frames) - 1)
      starts = offsets(frames)

      bytes =
        frames
        |> Enum.with_index()
        |> Enum.map(fn
          {frame, ^victim} -> damage(frame, damage, bit, at)
          {frame, _} -> frame
        end)
        |> IO.iodata_to_binary()

      forget_hooks!(config, 1..length(envs))
      put_wal!(config, bytes)

      file = wal_path(config)
      here = Enum.at(starts, victim)
      next = Enum.at(starts, victim + 1)

      assert {:error, {:damaged_legacy_wal, ^file, ^here, ^next}} = Migrate.run(config)
      assert hooks_present(config, 1..length(envs)) == []
      assert Store.get(config.instance, :default, Keys.meta("imported:wal")) == :not_found
      assert Store.get(config.instance, :default, Keys.meta("migration")) == :not_found
    end
  end

  property "any bytes as the WAL: the import ran, or the boot is refused naming the damage", %{
    config: config
  } do
    check all bytes <-
                one_of([
                  G.bytes(4096),
                  G.mutated(map(wal_envs(), &IO.iodata_to_binary(frames(&1)))),
                  map({wal_envs(), G.bytes(512)}, fn {envs, junk} ->
                    IO.iodata_to_binary(frames(envs)) <> junk
                  end),
                  map({G.bytes(512), wal_envs()}, fn {junk, envs} ->
                    junk <> IO.iodata_to_binary(frames(envs))
                  end),
                  map(G.bytes(300), &(<<0x484B::16, 1::8, 0::8>> <> &1))
                ]) do
      reset!(config)
      forget_hooks!(config, 1..6)
      put_wal!(config, bytes)
      file = wal_path(config)

      case Migrate.run(config) do
        :ok ->
          :ok

        {:error, {:damaged_legacy_wal, ^file, pos, later}} ->
          assert pos < later and later <= byte_size(bytes)
      end
    end
  end

  property "any bytes as a 0.3 term log import what decodes and stop, never raise", %{
    config: config
  } do
    check all file <-
                member_of([
                  {"dlq", "dlq.log"},
                  {"quarantine", "quarantine.log"},
                  {"segments", "index.log"}
                ]),
              bytes <-
                one_of([
                  G.bytes(2048),
                  G.mutated(
                    map(list_of(G.simple_term(), max_length: 4), fn terms ->
                      IO.iodata_to_binary(Enum.map(terms, &G.log_frame/1))
                    end)
                  ),
                  map(list_of(G.simple_term(), max_length: 4), fn terms ->
                    IO.iodata_to_binary(Enum.map(terms, &G.log_frame/1))
                  end),
                  map(G.bytes(64), &(<<0xFFFFFFFF::32>> <> &1)),
                  map({integer(0..4096), G.bytes(64)}, fn {len, rest} -> <<len::32>> <> rest end)
                ]) do
      reset!(config)
      {dir, name} = file
      path = Path.join([Config.path(config, dir), name])
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, bytes)

      assert Migrate.run(config) == :ok
    end
  end

  property "any bytes as a cursor or floor sidecar: read, or the boot is refused naming the file",
           %{config: config} do
    check all envs <- wal_envs(),
              sidecar <- member_of(["ankusa.wal.cursors", "ankusa.wal.truncated"]),
              bytes <-
                one_of([
                  G.bytes(128),
                  G.mutated(constant(:erlang.term_to_binary(%{dispatch: 3, compactor: 2}))),
                  G.mutated(constant(:erlang.term_to_binary(7))),
                  map(G.simple_term(), &:erlang.term_to_binary/1),
                  constant(:erlang.term_to_binary(%{dispatch: -1})),
                  constant(:erlang.term_to_binary(%{"dispatch" => 1})),
                  constant(:erlang.term_to_binary(-3)),
                  constant(:erlang.term_to_binary(%{dispatch: 1, compactor: 1}))
                ]) do
      reset!(config)
      forget_hooks!(config, 1..length(envs))
      put_wal!(config, IO.iodata_to_binary(frames(envs)))
      path = Path.join(Path.dirname(wal_path(config)), sidecar)
      File.write!(path, bytes)

      case Migrate.run(config) do
        :ok ->
          :ok

        {:error, {:corrupt_legacy_sidecar, ^path}} ->
          assert hooks_present(config, 1..length(envs)) == []
      end
    end
  end

  # One frame, damaged in a way that makes it fail to parse or fail its CRC.
  defp damage(frame, :payload, bit, at) do
    flip(frame, @header + rem(at, byte_size(frame) - @header), bit)
  end

  defp damage(frame, :crc, bit, at), do: flip(frame, 12 + rem(at, 4), bit)
  defp damage(frame, :length, bit, at), do: flip(frame, 16 + rem(at, 4), bit)
  defp damage(frame, :magic, bit, at), do: flip(frame, rem(at, 2), bit)
  defp damage(frame, :version, bit, _at), do: flip(frame, 2, bit)

  defp damage(frame, :zeroed, _bit, _at) do
    <<_::binary-size(@header), payload::binary>> = frame
    <<0::size(@header * 8), payload::binary>>
  end

  defp flip(frame, at, bit) do
    <<head::binary-size(^at), byte, tail::binary>> = frame
    <<head::binary, Bitwise.bxor(byte, Bitwise.bsl(1, bit)), tail::binary>>
  end
end
