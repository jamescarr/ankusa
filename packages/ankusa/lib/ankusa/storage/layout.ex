defmodule Ankusa.Storage.Layout do
  @moduledoc """
  Where archive objects live in the object store.

  A segment written by this release is

      <storage.key_prefix>archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first_seq>-<last_seq>.seg
      <storage.key_prefix>archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first_seq>-<last_seq>.idx
      <storage.key_prefix>archive/v1/dt=<yyyy-mm-dd>/<node>/_manifest.json

  `dt` is the UTC day of the segment's oldest hook (the hook id is a UUIDv7, so its
  time is in the id; a segment of ids that carry no time goes under the day it was
  written), `<node>` is `intake.node_id` or, without one, the host name, and the
  seqs are zero-padded to 20 digits. The day comes before the node so a node that
  has lost its catalogue can list one day's prefix and find every node's segments
  for it (`Queue.Archive.reconcile/2`, the `Ankusa.Storage.fetch/2`
  fallback) instead of needing the catalogue to say where to look.

  `_manifest.json` is a constant-size summary of one node's day: how many segments,
  their seq and id range and total bytes. It is rewritten after each segment and
  lets a reader skip a node whose ids cannot hold the one it wants.

  Segments an earlier release wrote, `seg/<first>-<last>.seg` (and `.idx`) under
  the prefix, are still found by the catalogue and by reconcile.
  """

  alias Ankusa.Config

  @root "archive/v1/"
  @day_ms 86_400_000

  @dated ~r"(?:\A|/)archive/v1/dt=(\d{4}-\d{2}-\d{2})/([A-Za-z0-9._-]+)/seg-(\d{20})-(\d{20})\.seg\z"
  @legacy ~r"(?:\A|/)seg/(\d{20})-(\d{20})\.seg\z"

  @type parsed :: %{
          kind: :dated | :legacy,
          key: String.t(),
          idx_key: String.t(),
          first: pos_integer(),
          last: pos_integer(),
          dt: String.t() | nil,
          node: String.t() | nil
        }

  @doc "The prefix every dated object is under (before `storage.key_prefix`)."
  @spec root() :: String.t()
  def root, do: @root

  @doc "The prefix of one day's objects, every node's."
  @spec day_prefix(String.t()) :: String.t()
  def day_prefix(dt), do: "#{@root}dt=#{dt}/"

  @doc "The name this node writes under: `intake.node_id`, else the host name, as a safe path part."
  @spec node_name(Config.t()) :: String.t()
  def node_name(%Config{intake: %{node_id: id}}) when is_binary(id) and id != "",
    do: sanitize(id)

  def node_name(%Config{}) do
    {:ok, host} = :inet.gethostname()
    sanitize(List.to_string(host))
  end

  defp sanitize(name) do
    name = name |> String.replace(~r/[^A-Za-z0-9._-]/, "_") |> String.slice(0, 63)
    if name == "" or String.match?(name, ~r/\A\.+\z/), do: "node", else: name
  end

  @doc "The UTC day (`yyyy-mm-dd`) of an event id's time; today when the id carries none."
  @spec day(String.t(), integer()) :: String.t()
  def day(id, now_ms \\ System.system_time(:millisecond)) do
    ms =
      case Ankusa.UUIDv7.timestamp_ms(id) do
        {:ok, ms} -> ms
        :error -> now_ms
      end

    date(ms) || date(now_ms)
  end

  @doc "The days a fetch looks in for an id: its own and the day before; `:error` for an id with no time."
  @spec days_for(String.t()) :: [String.t()] | :error
  def days_for(id) do
    case Ankusa.UUIDv7.timestamp_ms(id) do
      {:ok, ms} -> [date(ms), date(ms - @day_ms)] |> Enum.reject(&is_nil/1) |> Enum.uniq()
      :error -> :error
    end
  end

  defp date(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, at} -> at |> DateTime.to_date() |> Date.to_iso8601()
      {:error, _reason} -> nil
    end
  end

  @doc "Object name (no prefix, no extension) of the segment `first`..`last` of `node` on `dt`."
  @spec segment_name(String.t(), String.t(), pos_integer(), pos_integer()) :: String.t()
  def segment_name(dt, node, first, last),
    do: "#{@root}dt=#{dt}/#{node}/seg-#{pad(first)}-#{pad(last)}"

  @doc "Object name (no prefix) of the day manifest of `node` on `dt`."
  @spec manifest_name(String.t(), String.t()) :: String.t()
  def manifest_name(dt, node), do: "#{@root}dt=#{dt}/#{node}/_manifest.json"

  @doc "The full key of the manifest beside the dated segment `seg_key`."
  @spec manifest_key(String.t()) :: String.t()
  def manifest_key(seg_key),
    do: String.replace(seg_key, ~r"/seg-\d{20}-\d{20}\.(seg|idx)\z", "/_manifest.json")

  @doc "Parse a listed key: a dated segment, a legacy one, or `nil` for anything else."
  @spec parse(String.t()) :: parsed() | nil
  def parse(key) do
    cond do
      match = Regex.run(@dated, key) ->
        [_all, dt, node, first, last] = match

        %{
          kind: :dated,
          key: key,
          idx_key: String.replace_suffix(key, ".seg", ".idx"),
          first: String.to_integer(first),
          last: String.to_integer(last),
          dt: dt,
          node: node
        }

      match = Regex.run(@legacy, key) ->
        [_all, first, last] = match

        %{
          kind: :legacy,
          key: key,
          idx_key: String.replace_suffix(key, ".seg", ".idx"),
          first: String.to_integer(first),
          last: String.to_integer(last),
          dt: nil,
          node: nil
        }

      true ->
        nil
    end
  end

  # ── the day manifest ─────────────────────────────────────────────────────

  @doc "An empty manifest for `node` on `dt`."
  @spec new_manifest(String.t(), String.t()) :: map()
  def new_manifest(dt, node),
    do: %{
      "format" => 1,
      "dt" => dt,
      "node" => node,
      "segments" => 0,
      "bytes" => 0,
      "first_seq" => nil,
      "last_seq" => 0,
      "min_id" => nil,
      "max_id" => nil
    }

  @doc "The manifest with one more segment (a catalogue `row`) counted in."
  @spec add_segment(map(), map()) :: map()
  def add_segment(manifest, row) do
    %{
      manifest
      | "segments" => manifest["segments"] + 1,
        "bytes" => manifest["bytes"] + row.bytes,
        "first_seq" => manifest["first_seq"] || row.first_seq,
        "last_seq" => max(manifest["last_seq"], row.last_seq),
        "min_id" => min_id(manifest["min_id"], row.min_id),
        "max_id" => max_id(manifest["max_id"], row.max_id)
    }
  end

  defp min_id(nil, id), do: id
  defp min_id(a, b), do: min(a, b)
  defp max_id(nil, id), do: id
  defp max_id(a, b), do: max(a, b)

  @doc "Decode a manifest object; `{:error, :bad_manifest}` for anything that is not one."
  @spec decode_manifest(binary()) :: {:ok, map()} | {:error, :bad_manifest}
  def decode_manifest(bin) do
    case JSON.decode(bin) do
      {:ok, %{"format" => 1, "last_seq" => last, "segments" => n} = manifest}
      when is_integer(last) and is_integer(n) ->
        {:ok, manifest}

      _ ->
        {:error, :bad_manifest}
    end
  end

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")
end
