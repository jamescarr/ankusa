defmodule Ankusa.BlobStore.LocalFS do
  @moduledoc """
  Default `Ankusa.BlobStore`: immutable objects on the local filesystem.

  Objects live under `opts[:root]` (an absolute path), by default
  `Config.path(config, "segments")`. A dedicated claim store
  (`claim_check.blob_stores: [{LocalFS, root: "/shared/claims"}]`) points it
  elsewhere — at a directory every node and the gateway mount, say.

  Writes are atomic (temp file + rename) and durable (file and directory
  fsyncs), so a reader never sees a half-written object and a committed one
  survives power loss. Reads use `:file.pread/3` for a single-record range
  `GET` without slurping the whole object into memory.

  ## Object operations

  The optional object callbacks keep each object's content type, metadata and
  version in a sidecar document at `<root>/.ankusa-meta/<key>.json` (data file
  first, then the sidecar, each written to a temp name and renamed). A version
  is a fresh ULID per write; an object with no sidecar, or one whose recorded
  size no longer matches the data file (written out of band or interrupted
  between the two renames), reports `"x" <> lowercase hex sha256` of its bytes
  instead. `:if` conditions are checked and the write made under a per-key
  `:global.trans/2` lock, so a compare-and-swap is exclusive within one node
  (or one connected cluster).

  This is a development and single-node store: the two renames are not
  crash-atomic (a same-size rewrite interrupted between them keeps the old
  version on new bytes), and the lock is not shared with unclustered nodes that
  mount the same directory.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.{Config, Fsync, ULID}

  @object_default_content_type "application/octet-stream"
  @meta_dir ".ankusa-meta"

  @impl true
  def put(instance, key, data, opts) do
    root = root(instance, opts)
    path = Path.join(root, key)

    with :ok <- Ankusa.Fsync.mkdir_p(Path.dirname(path), root) do
      Ankusa.Fsync.write_file(path, data)
    end
  end

  # Not atomic across processes (the file is written to a temporary name and
  # renamed over `key`, which replaces): one node's keys are unique by
  # construction, and a claim pack's id carries 64 random bits.
  @impl true
  def put_if_absent(instance, key, data, opts) do
    if File.exists?(Path.join(root(instance, opts), key)) do
      Ankusa.BlobStore.same_object(__MODULE__, instance, key, data, opts)
    else
      put(instance, key, data, opts)
    end
  end

  @impl true
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  def get(instance, key, opts) do
    case File.read(Path.join(root(instance, opts), key)) do
      {:ok, bin} -> {:ok, bin}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    case :file.open(Path.join(root(instance, opts), key), [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset, length)
        :file.close(fd)

        case result do
          {:ok, bin} -> {:ok, bin}
          :eof -> {:error, :eof}
          {:error, reason} -> {:error, reason}
        end

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  def delete(instance, key, opts) do
    _ = File.rm(Path.join(root(instance, opts), key))
    :ok
  end

  # A root that does not exist yet holds no keys.
  @impl true
  def list(instance, prefix, opts) do
    root = root(instance, opts)

    keys =
      root
      |> Path.join(prefix_dir(prefix))
      |> Path.join("**")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&Path.relative_to(&1, root))
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()

    {:ok, keys}
  end

  # ── object operations ──────────────────────────────────────────────────────

  @impl true
  def head_object(instance, key, opts) do
    root = root(instance, opts)

    with {:ok, stat} <- stat(root, key) do
      {:ok, object_meta(root, key, stat)}
    end
  end

  @impl true
  def get_object(instance, key, opts) do
    root = root(instance, opts)
    path = Path.join(root, key)

    with {:ok, stat} <- stat(root, key),
         meta = object_meta(root, key, stat),
         :ok <- check_version(meta.version, opts[:if]),
         {:ok, first, last} <- object_span(opts[:range], meta.size),
         :ok <- check_max_bytes(last - first + 1, opts[:max_bytes]),
         {:ok, body} <- read_span(path, first, last) do
      {:ok, meta, body}
    end
  end

  @impl true
  def put_object(instance, key, data, opts) do
    root = root(instance, opts)
    bytes = IO.iodata_to_binary(data)

    :global.trans({{__MODULE__, root, key}, self()}, fn ->
      with :ok <- put_condition(root, key, opts[:if]),
           version = new_version(),
           :ok <- write_object(root, key, bytes, version, opts) do
        {:ok, version}
      end
    end)
  end

  @impl true
  # `root` is operator config; the key is validated by the caller (no `..`,
  # no backslash, no absolute segment).
  # sobelow_skip ["Traversal.FileModule"]
  def delete_object(instance, key, opts) do
    root = root(instance, opts)
    _ = File.rm(Path.join(root, key))
    _ = File.rm(sidecar_path(root, key))
    :ok
  end

  @impl true
  def list_page(instance, prefix, opts) do
    root = root(instance, opts)

    {page, rest} =
      root
      |> object_keys(prefix)
      |> Enum.map(&page_entry(root, &1, prefix, opts[:delimiter]))
      |> Enum.uniq_by(&elem(&1, 0))
      |> Enum.sort()
      |> drop_through(opts[:cursor])
      |> Enum.split(limit(opts))

    {:ok,
     %{
       objects: objects(page),
       prefixes: prefixes(page),
       next: if(rest == [], do: nil, else: page |> List.last() |> elem(0))
     }}
  end

  # ── object operation internals ─────────────────────────────────────────────

  defp stat(root, key) do
    case File.stat(Path.join(root, key), time: :posix) do
      {:ok, stat} -> {:ok, stat}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp object_meta(root, key, stat) do
    size = stat.size

    case read_sidecar(root, key) do
      {:ok, %{"version" => version, "size" => ^size} = doc} when is_binary(version) ->
        %{
          size: size,
          version: version,
          content_type: doc["content_type"],
          last_modified: stat.mtime,
          metadata: metadata_of(doc["metadata"])
        }

      _absent_or_stale ->
        %{
          size: size,
          version: content_version(Path.join(root, key)),
          content_type: nil,
          last_modified: stat.mtime,
          metadata: %{}
        }
    end
  end

  defp metadata_of(%{} = metadata) do
    for {name, value} <- metadata,
        is_binary(name) and is_binary(value),
        into: %{},
        do: {name, value}
  end

  defp metadata_of(_metadata), do: %{}

  # The version of an object this store did not write: a hash of its bytes.
  # The path is operator config plus a caller-validated key.
  # sobelow_skip ["Traversal.FileModule"]
  defp content_version(path) do
    case File.read(path) do
      {:ok, bytes} -> "x" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
      {:error, _reason} -> "x" <> Base.encode16(:crypto.hash(:sha256, ""), case: :lower)
    end
  end

  defp check_version(_version, nil), do: :ok
  defp check_version(version, {:version, version}), do: :ok
  defp check_version(_version, {:version, _expected}), do: {:error, :version_mismatch}

  defp object_span(nil, 0), do: {:ok, 0, -1}
  defp object_span(nil, size), do: {:ok, 0, size - 1}

  defp object_span({first, _last}, size) when first >= size,
    do: {:error, :range_not_satisfiable}

  defp object_span({first, last}, _size), do: {:ok, first, last}

  defp check_max_bytes(_bytes, nil), do: :ok
  defp check_max_bytes(bytes, max) when bytes > max, do: {:error, :too_large}
  defp check_max_bytes(_bytes, _max), do: :ok

  defp read_span(_path, first, last) when last < first, do: {:ok, ""}

  defp read_span(path, first, last) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, first, last - first + 1)
        :file.close(fd)

        case result do
          {:ok, body} -> {:ok, body}
          :eof -> {:error, :range_not_satisfiable}
          {:error, reason} -> {:error, reason}
        end

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_condition(_root, _key, nil), do: :ok

  defp put_condition(root, key, :absent) do
    if File.exists?(Path.join(root, key)), do: {:error, :already_exists}, else: :ok
  end

  defp put_condition(root, key, {:version, expected}) do
    case stat(root, key) do
      {:ok, stat} ->
        if current_version(root, key, stat.size) == expected,
          do: :ok,
          else: {:error, :version_mismatch}

      {:error, :not_found} ->
        {:error, :version_mismatch}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp current_version(root, key, size) do
    case read_sidecar(root, key) do
      {:ok, %{"version" => version, "size" => ^size}} when is_binary(version) -> version
      _absent_or_stale -> content_version(Path.join(root, key))
    end
  end

  defp new_version do
    ULID.encode(<<System.system_time(:millisecond)::48, :crypto.strong_rand_bytes(10)::binary>>)
  end

  defp write_object(root, key, bytes, version, opts) do
    content_type = opts[:content_type] || @object_default_content_type
    metadata = opts[:metadata] || %{}
    path = Path.join(root, key)
    sidecar = sidecar_path(root, key)

    doc = %{
      "version" => version,
      "size" => byte_size(bytes),
      "content_type" => content_type,
      "metadata" => metadata
    }

    with :ok <- Fsync.mkdir_p(Path.dirname(path), root),
         :ok <- Fsync.write_file(path, bytes),
         :ok <- Fsync.mkdir_p(Path.dirname(sidecar), root) do
      Fsync.write_file(sidecar, JSON.encode!(doc))
    end
  end

  defp sidecar_path(root, key), do: Path.join([root, @meta_dir, key <> ".json"])

  # The path is operator config plus a caller-validated key.
  # sobelow_skip ["Traversal.FileModule"]
  defp read_sidecar(root, key) do
    with {:ok, body} <- File.read(sidecar_path(root, key)),
         {:ok, %{} = doc} <- JSON.decode(body) do
      {:ok, doc}
    else
      _missing_or_malformed -> :error
    end
  end

  # Sorted keys under `prefix`, never the sidecar tree.
  defp object_keys(root, prefix) do
    root
    |> Path.join(prefix_dir(prefix))
    |> Path.join("**")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.relative_to(&1, root))
    |> Enum.filter(&(String.starts_with?(&1, prefix) and not sidecar_key?(&1)))
    |> Enum.sort()
  end

  defp sidecar_key?(key), do: String.starts_with?(key, @meta_dir <> "/")

  defp page_entry(root, key, prefix, "/") do
    case String.split(String.replace_prefix(key, prefix, ""), "/", parts: 2) do
      [_name] -> {key, {:object, key, object_size(root, key)}}
      [dir, _rest] -> {prefix <> dir <> "/", {:prefix, prefix <> dir <> "/"}}
    end
  end

  defp page_entry(root, key, _prefix, _delimiter),
    do: {key, {:object, key, object_size(root, key)}}

  defp object_size(root, key) do
    case File.stat(Path.join(root, key)) do
      {:ok, stat} -> stat.size
      {:error, _reason} -> 0
    end
  end

  defp drop_through(entries, nil), do: entries
  defp drop_through(entries, cursor), do: Enum.drop_while(entries, &(elem(&1, 0) <= cursor))

  defp objects(page) do
    for {_sort, {:object, key, size}} <- page, do: %{key: key, size: size}
  end

  defp prefixes(page) do
    for {_sort, {:prefix, prefix}} <- page, do: prefix
  end

  defp limit(opts) do
    case opts[:limit] do
      limit when is_integer(limit) -> limit |> max(1) |> min(1000)
      _default -> 1000
    end
  end

  @doc "The directory objects live under: `opts[:root]`, else the instance's `segments` dir."
  @spec root(atom(), keyword()) :: String.t()
  def root(instance, opts) do
    case Keyword.get(opts, :root) do
      nil -> Config.path(Ankusa.config(instance), "segments")
      root -> root
    end
  end

  # Walk only the prefix's containing directory, not the whole store root —
  # a deep prefix (`claims/<tenant>/`) shouldn't have to glob every segment
  # to find its own keys. A bare prefix with no `/` (e.g. `"seg"`) still
  # walks the whole root, same as before.
  defp prefix_dir(prefix) do
    case Path.dirname(prefix) do
      "." -> ""
      dir -> dir
    end
  end
end
