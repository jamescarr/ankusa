defmodule Ankusa.Property.CorruptFramesTest do
  @moduledoc """
  The checked-in corpus under `test/fixtures/corrupt_frames/`: torn and
  bit-flipped 0.3 WAL frames, and sidecars that are not what 0.3 wrote. Each
  one has acked data (a valid frame) beyond the damage, or a cursor that cannot
  be trusted, so the import must refuse to start with `{:error, _}` instead of
  importing around it. Every file is read as bytes and replayed against a fresh
  store; `README.md` there says what each one is.

  A torn *final* frame is the opposite case and is forgiven; that one is
  generated, not checked in (`migrate_property_test.exs`).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Envelope, Store}
  alias Ankusa.Store.{Keys, Migrate}
  alias Ankusa.Test.LegacyDataDir, as: Legacy

  @moduletag capture_log: true

  @corpus Path.expand("../../fixtures/corrupt_frames", __DIR__)
  @sidecars %{".cursors" => "ankusa.wal.cursors", ".truncated" => "ankusa.wal.truncated"}

  # The corpus is not allowed to shrink unnoticed: a deleted or renamed file
  # would otherwise just stop being tested.
  @expected ~w(
    bitflip_crc_then_valid.wal
    bitflip_length_then_valid.wal
    bitflip_magic_then_valid.wal
    bitflip_payload_then_valid.wal
    bitflip_version_then_valid.wal
    cursors_garbage.cursors
    cursors_negative.cursors
    cursors_torn.cursors
    cursors_wrong_shape.cursors
    damage_in_second_of_three.wal
    floor_negative.truncated
    floor_not_integer.truncated
    garbage_run_then_valid.wal
    torn_frame_glued_to_next.wal
    zero_page_then_valid.wal
    zero_extended_header_then_valid.wal
    zeroed_header_then_valid.wal
  )

  setup do
    config = test_config(roles: [:edge, :dispatch, :storage], storage: %{interval_ms: 0})
    put_config(config)
    start_supervised!({Store, instance: config.instance})
    %{config: config}
  end

  test "the corpus is all there" do
    on_disk = @corpus |> File.ls!() |> Enum.reject(&(&1 == "README.md")) |> Enum.sort()
    assert on_disk == Enum.sort(@expected)
  end

  for name <- @expected do
    test "#{name} refuses the import", %{config: config} do
      file = Path.join(@corpus, unquote(name))
      extension = Path.extname(file)

      case extension do
        ".wal" ->
          File.mkdir_p!(Config.path(config, "wal"))
          File.cp!(file, Path.join(Config.path(config, "wal"), "ankusa.wal"))

        sidecar ->
          # A trustworthy WAL beside the damaged sidecar: it is the sidecar that refuses.
          env = %Envelope{
            id: "00000000-0000-7000-8000-000000000001",
            source_id: "demo",
            received_at: 1_790_258_591_000,
            method: "POST",
            path: "/webhooks/demo",
            headers: [],
            body: "ok",
            size: 2,
            seq: 1
          }

          Legacy.wal!(config, [env])
          File.cp!(file, Path.join(Config.path(config, "wal"), Map.fetch!(@sidecars, sidecar)))
      end

      # The empty store `setup` booted already ran its first-boot import and
      # marked it done; a 0.3 data dir is only read before that marker.
      :ok = Store.write(config.instance, [{:delete, :default, Keys.meta("migration")}])

      assert {:error, reason} = Migrate.run(config)
      assert elem(reason, 0) in [:damaged_legacy_wal, :corrupt_legacy_sidecar]

      # Nothing was imported and nothing was marked done: the node stays unbootable, not half-migrated.
      assert Store.get(config.instance, :default, Keys.meta("migration")) == :not_found
      assert Store.get(config.instance, :default, Keys.meta("imported:wal")) == :not_found
      assert Store.get(config.instance, :hooks, Keys.hook(1)) == :not_found
    end
  end
end
