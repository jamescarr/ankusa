defmodule Ankusa.BlobStore.Options do
  @moduledoc """
  The transport options every HTTP object store takes (`Ankusa.BlobStore.S3`,
  `GCS`, `Azure`, `OCI`), their defaults and their ranges. They are adapter
  options, like `:timeout_ms` and `:retries`: Elixir config, and the `s3`/`gcs`
  sections of the YAML.

  | Option | Default | What |
  | --- | --- | --- |
  | `:timeout_ms` | `10_000` | One metadata-sized request (`GET`, `DELETE`, `LIST`, a body up to 1 MiB): connect and response |
  | `:upload_timeout_ms` | `60_000` | One request that carries a large body: a bigger single `PUT`, or one multipart part |
  | `:op_deadline_ms` | `120_000` | **One absolute deadline per blob operation**, covering every attempt, every multipart part and (for `Ankusa.ClaimCheck.check_in/4`) every pack-id retry together. Nothing sleeps or sends past it |
  | `:multipart_threshold_bytes` | `8_388_608` | A `put` of more bytes goes up as a multipart (S3, OCI), block-list (Azure) or resumable (GCS) upload |
  | `:multipart_part_bytes` | `8_388_608` | The size of each part or chunk (a multiple of 256 KiB, at least 5 MiB) |
  | `:retries` | `2` | Extra attempts after a transient failure (`Ankusa.BlobStore.Retry`) |
  | `:breaker_failures` | `5` | Consecutive failures of a `{endpoint, bucket}` that open its breaker; `0` turns the breaker off, the latency trip with it |
  | `:breaker_open_ms` | `30_000` | How long an open breaker answers `{:error, :breaker_open}` before it lets one request through |
  | `:breaker_slow_ms` | `5_000` | The breaker also opens when the p99 of the last 100 metadata-sized requests passes this |
  | `:pool_size`, `:pool_count` | `32`, `1` | Connections per pool and pools, of the Finch pool the instance starts for this store's endpoint (`Ankusa.BlobStore.Pool`) |
  | `:pool_timeout` | `5_000` | How long a request waits for a free connection of that pool |

  Every bound is checked when the config is validated (`validate!/3`), so a bad
  value is a boot error and never a request that waits an hour.
  """

  @mib 1_048_576

  @defaults %{
    timeout_ms: 10_000,
    upload_timeout_ms: 60_000,
    op_deadline_ms: 120_000,
    multipart_threshold_bytes: 8 * @mib,
    multipart_part_bytes: 8 * @mib,
    retries: 2,
    breaker_failures: 5,
    breaker_open_ms: 30_000,
    breaker_slow_ms: 5_000,
    pool_size: 32,
    pool_count: 1,
    pool_timeout: 5_000
  }

  @ranges %{
    timeout_ms: {1, 600_000},
    upload_timeout_ms: {1_000, 3_600_000},
    op_deadline_ms: {1_000, 3_600_000},
    multipart_threshold_bytes: {1, 4_294_967_296},
    multipart_part_bytes: {5 * @mib, 536_870_912},
    retries: {0, 10},
    breaker_failures: {0, 1_000},
    breaker_open_ms: {100, 3_600_000},
    breaker_slow_ms: {10, 600_000},
    pool_size: {1, 1_024},
    pool_count: {1, 64},
    pool_timeout: {1, 600_000}
  }

  # A body this small is "metadata-sized": its latency is a sample for the
  # breaker and it runs under `:timeout_ms`.
  @small_body_bytes @mib

  @adapters [
    Ankusa.BlobStore.S3,
    Ankusa.BlobStore.GCS,
    Ankusa.BlobStore.Azure,
    Ankusa.BlobStore.OCI
  ]

  @doc "The option's default."
  @spec default(atom()) :: pos_integer() | non_neg_integer()
  def default(key), do: Map.fetch!(@defaults, key)

  @doc "`opts[key]`, else the default."
  @spec get(keyword(), atom()) :: non_neg_integer()
  def get(opts, key), do: Keyword.get(opts, key, Map.fetch!(@defaults, key))

  @doc """
  `:small` for a request that carries at most 1 MiB, `:bulk` for one that
  carries more: which timeout it runs under, and whether its latency is a
  breaker sample.
  """
  @spec kind(non_neg_integer()) :: :small | :bulk
  def kind(size) when size <= @small_body_bytes, do: :small
  def kind(_size), do: :bulk

  @doc """
  `opts` with `:deadline` (an absolute `System.monotonic_time(:millisecond)`)
  set to `op_deadline_ms` from now, or to the deadline the caller already
  passed when that is sooner. Every adapter entry point calls it once, so every
  request the operation makes shares one deadline.
  """
  @spec with_deadline(keyword()) :: keyword()
  def with_deadline(opts) do
    own = System.monotonic_time(:millisecond) + get(opts, :op_deadline_ms)
    Keyword.put(opts, :deadline, min(own, Keyword.get(opts, :deadline, own)))
  end

  @doc "Milliseconds left to `opts[:deadline]` (negative when passed)."
  @spec remaining(keyword()) :: integer()
  def remaining(opts), do: Keyword.fetch!(opts, :deadline) - System.monotonic_time(:millisecond)

  @doc """
  Raise `ArgumentError` naming `where` unless every transport option `opts`
  sets is an integer inside its range, and the part size is a multiple of
  256 KiB. Stores that are not an HTTP object store are not checked.
  """
  @spec validate!(module(), keyword(), String.t()) :: :ok
  def validate!(module, opts, where) when module in @adapters and is_list(opts) do
    Enum.each(@ranges, fn {key, {min, max}} ->
      case Keyword.fetch(opts, key) do
        :error ->
          :ok

        {:ok, value} when is_integer(value) and value >= min and value <= max ->
          :ok

        {:ok, value} ->
          raise ArgumentError,
                "#{where}: #{key} must be an integer from #{min} to #{max}, got #{inspect(value)}"
      end
    end)

    part = get(opts, :multipart_part_bytes)

    if rem(part, 262_144) != 0 do
      raise ArgumentError,
            "#{where}: multipart_part_bytes must be a multiple of 262144 (256 KiB), got #{part}"
    end

    :ok
  end

  def validate!(_module, _opts, _where), do: :ok
end
