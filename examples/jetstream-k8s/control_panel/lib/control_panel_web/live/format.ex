defmodule ControlPanelWeb.DashboardLive.Format do
  @moduledoc """
  Pure helpers for the dashboard: the size slider's log scale, humanized
  numbers and sizes, timestamps, and pod names.
  """

  @min_size 64
  @max_size 4 * 1024 * 1024

  @doc """
  The size slider's position (0-100) as bytes: a log scale from 64 B to 4 MiB,
  so one slider covers an inline hook and a claimed one with equal resolution.
  """
  @spec pos_to_bytes(number()) :: pos_integer()
  def pos_to_bytes(pos), do: round(@min_size * :math.pow(@max_size / @min_size, pos / 100))

  @doc "The inverse of `pos_to_bytes/1`."
  @spec bytes_to_pos(number()) :: 0..100
  def bytes_to_pos(bytes) do
    pos = 100 * :math.log(max(bytes, @min_size) / @min_size) / :math.log(@max_size / @min_size)
    pos |> round() |> max(0) |> min(100)
  end

  @doc "512 B, 4.0 KiB, 1.5 MiB."
  @spec bytes(number() | nil) :: String.t()
  def bytes(nil), do: "–"
  def bytes(n) when n < 1_024, do: "#{round(n)} B"
  def bytes(n) when n < 1_024 * 1_024, do: "#{Float.round(n / 1_024, 1)} KiB"
  def bytes(n), do: "#{Float.round(n / (1_024 * 1_024), 1)} MiB"

  @doc "12,345."
  @spec int(number() | nil) :: String.t()
  def int(nil), do: "–"

  def int(n) do
    n
    |> round()
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  @doc "A rate with one decimal below 10, none above: 0.4, 12."
  @spec rate(number() | nil) :: String.t()
  def rate(nil), do: "0"
  def rate(r) when r == 0, do: "0"
  def rate(r) when r < 10, do: "#{Float.round(r * 1.0, 1)}"
  def rate(r), do: "#{round(r)}"

  @doc "HH:MM:SS.mmm (UTC) of a millisecond Unix timestamp."
  @spec clock(integer() | nil) :: String.t()
  def clock(nil), do: "–"

  def clock(ms) do
    time = ms |> DateTime.from_unix!(:millisecond) |> Calendar.strftime("%H:%M:%S")
    "#{time}.#{ms |> rem(1_000) |> Integer.to_string() |> String.pad_leading(3, "0")}"
  end

  @doc "The last `n` characters of a hook id."
  @spec tail_id(String.t() | nil, pos_integer()) :: String.t()
  def tail_id(nil, _n), do: "–"
  def tail_id(id, n), do: String.slice(id, -n, n)

  @doc """
  `ankusa-7d9f8b6c5-x2k4j` -> `ankusa·x2k4j`: the Deployment's ReplicaSet hash
  tells nobody anything. A StatefulSet pod (`nats-0`, `ankusa-durable-0`)
  keeps its full name.
  """
  @spec short_name(String.t(), String.t()) :: String.t()
  def short_name(name, group) when group in ["nats", "ankusa-durable"], do: name
  def short_name(name, group), do: "#{group}·#{name |> String.split("-") |> List.last()}"
end
