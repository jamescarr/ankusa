defmodule Ankusa.Gateway.Objects do
  @moduledoc """
  The objects API: buffered `GET`/`HEAD`/`PUT`/`DELETE` of one object and one
  page of a listing, in a space (`Ankusa.Gateway.Spaces`) of
  `gateway.spaces`.

  Every request: the space (`404`), the key (`400 invalid_key`), the space's
  policy (`403`, `503 authz_unavailable`), a slot among
  `gateway.max_concurrent_requests` (`503 overloaded`), then the store. Object
  bytes held in memory count against `gateway.objects.max_inflight_bytes`
  (`Ankusa.Gateway.Objects.Budget`, `503 overloaded` past it).

    * `GET /v1/objects/{space}/{key...}` (`read`) — one store read of up to
      `read_first_bytes` from the first byte asked for, which also yields the
      size and the version; the rest, when there is any, in a second read held
      to that version (an object overwritten in between restarts the read
      once, then `503 store_unavailable`). One `bytes` range (`206`, `416`),
      `If-None-Match` (`304`), `422 object_too_large` past
      `max_object_bytes`.
    * `HEAD` the same (`read`) — the headers of a `GET`, no body.
    * `PUT` the same (`write`) — `content-length` required (`411`), at most
      `max_object_bytes` (`413`); `x-ankusa-meta-<name>` headers are its
      metadata (`400 invalid_metadata`); `if-none-match: *` and
      `if-match: "<version>"` are conditions (`412 precondition_failed`).
      `200 {"space","key","size","version"}`.
    * `DELETE` the same (`delete`) — `204`.
    * `GET /v1/objects/{space}?prefix=&delimiter=&cursor=&limit=` (`list`, on
      the prefix) — `{"objects":[{"key","size"}],"prefixes":[],"next_cursor"}`.
      Authorizing `list` on a prefix shows every key under it, whatever
      `read` says about each.

  `PUT` and `DELETE` on a `read_only` space are `405`. Objects are served as
  attachments, sandboxed, never sniffed: whatever the stored content type, a
  browser does not run them.

  Emits `[:ankusa, :gateway, :request]` with `api: :objects` for every answer.
  """

  alias Ankusa.ClaimCheck.Reader
  alias Ankusa.Gateway.{Authz, Config, Range, Spaces}
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.Objects.Budget
  alias Ankusa.{Http, Telemetry}

  @max_metadata 16
  @metadata_name ~r/\A[a-z0-9-]{1,64}\z/
  @max_metadata_value 1024
  @max_cursor 2048
  @default_limit 100
  @default_content_type "application/octet-stream"

  @doc "Serve a request on one object: `GET`, `HEAD`, `PUT` or `DELETE`."
  @spec object(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def object(conn, space_name) do
    timed(fn ->
      with {:ok, action} <- action(conn),
           {:ok, gateway, space} <- space(conn, space_name),
           {:ok, key} <- key(conn) do
        authorized(conn, gateway, space, action, %{kind: :object, key: key}, fn conn ->
          serve(conn, gateway, space, key)
        end)
      else
        {:error, conn} -> conn
      end
    end)
  end

  @doc "Serve one page of the listing of `space`."
  @spec list(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def list(conn, space_name) do
    timed(fn ->
      conn = Plug.Conn.fetch_query_params(conn)

      with {:ok, gateway, space} <- space(conn, space_name),
           {:ok, query} <- list_query(conn) do
        authorized(conn, gateway, space, :list, %{kind: :prefix, key: query.prefix}, fn conn ->
          list_page(conn, gateway, space, query)
        end)
      else
        {:error, conn} -> conn
      end
    end)
  end

  # ── the pipeline ────────────────────────────────────────────────────────────

  defp timed(fun) do
    started = System.monotonic_time()
    conn = fun.()

    Telemetry.emit(
      [:gateway, :request],
      %{duration: System.monotonic_time() - started},
      %{instance: instance(conn), api: :objects, method: conn.method, status: conn.status}
    )

    conn
  end

  defp action(%Plug.Conn{method: "GET"}), do: {:ok, :read}
  defp action(%Plug.Conn{method: "HEAD"}), do: {:ok, :read}
  defp action(%Plug.Conn{method: "PUT"}), do: {:ok, :write}
  defp action(%Plug.Conn{method: "DELETE"}), do: {:ok, :delete}

  defp action(conn),
    do: {:error, Http.send_json(conn, 405, %{error: "method_not_allowed"})}

  defp space(conn, name) do
    gateway = Ankusa.Gateway.config(instance(conn))

    case Spaces.fetch(gateway, name) do
      {:ok, space} -> {:ok, gateway, space}
      :error -> {:error, Http.send_json(conn, 404, %{error: "not_found"})}
    end
  end

  # The raw segments after `/v1/objects/<space>/`, from `conn.request_path`:
  # it is not percent-decoded (`Spaces.key_from_segments/1` decodes each
  # segment exactly once) and, unlike `conn.path_info`, keeps empty segments,
  # so `a//b` is refused rather than read as `a/b`.
  defp key(conn) do
    segments =
      case String.split(conn.request_path, "/") do
        ["", "v1", "objects", _space | rest] -> rest
        _ -> []
      end

    case Spaces.key_from_segments(segments) do
      {:ok, key} -> {:ok, key}
      :error -> {:error, Http.send_json(conn, 400, %{error: "invalid_key"})}
    end
  end

  defp authorized(conn, gateway, space, action, resource, serve) do
    instance = gateway.instance
    resource = Map.merge(resource, %{space: space.name, tenant: nil})

    case Authz.decide(instance, space.policy, Request.from_conn(conn, action, resource)) do
      {:allow, _meta} ->
        admitted(conn, gateway, serve)

      {:deny, meta} ->
        Http.send_json(conn, 403, forbidden(meta))

      {:error, {:unavailable, _reason}} ->
        conn
        |> retry_after()
        |> Http.send_json(503, %{error: "authz_unavailable"})
    end
  end

  defp forbidden(%{scope: scope}) when is_binary(scope), do: %{error: "forbidden", scope: scope}
  defp forbidden(_meta), do: %{error: "forbidden"}

  defp admitted(conn, gateway, serve) do
    instance = gateway.instance

    case Reader.admit(instance, gateway.max_concurrent_requests) do
      :ok ->
        try do
          serve.(conn)
        after
          Budget.release(instance)
          Reader.release(instance)
        end

      :shed ->
        overloaded(conn)
    end
  end

  # ── one object ──────────────────────────────────────────────────────────────

  defp serve(%Plug.Conn{method: "GET"} = conn, gateway, space, key),
    do: get(conn, gateway, space, key, 1)

  defp serve(%Plug.Conn{method: "HEAD"} = conn, gateway, space, key) do
    {module, _opts} = space.store
    opts = Config.store_opts(gateway, space)

    case module.head_object(gateway.instance, Spaces.object_key(space, key), opts) do
      {:ok, meta} ->
        conn
        |> object_headers(meta)
        |> Plug.Conn.put_resp_header("content-length", Integer.to_string(meta.size))
        |> Plug.Conn.send_resp(200, "")

      {:error, reason} ->
        Ankusa.Gateway.Http.store_error(conn, reason)
    end
  end

  defp serve(%Plug.Conn{method: method} = conn, _gateway, %{read_only: true}, _key)
       when method in ["PUT", "DELETE"],
       do: Http.send_json(conn, 405, %{error: "method_not_allowed"})

  defp serve(%Plug.Conn{method: "PUT"} = conn, gateway, space, key),
    do: put(conn, gateway, space, key)

  defp serve(%Plug.Conn{method: "DELETE"} = conn, gateway, space, key) do
    {module, _opts} = space.store
    opts = Config.store_opts(gateway, space)

    case module.delete_object(gateway.instance, Spaces.object_key(space, key), opts) do
      :ok -> Plug.Conn.send_resp(conn, 204, "")
      {:error, reason} -> Ankusa.Gateway.Http.store_error(conn, reason)
    end
  end

  # ── GET ─────────────────────────────────────────────────────────────────────

  defp get(conn, gateway, space, key, attempt) do
    parsed = Range.parse(conn)

    with {:ok, start, cap, held} <- start_of(conn, gateway, space, key, parsed),
         :ok <- reserve(gateway, space.read_first_bytes),
         {:ok, meta, first} <-
           first_read(conn, gateway, space, key, parsed, {start, cap, held}) do
      case Range.resolve(parsed, meta.size) do
        :unsatisfiable -> unsatisfiable(conn, meta.size)
        resolved -> respond(conn, {gateway, space, key}, {meta, first, start}, resolved, attempt)
      end
    else
      {:error, :shed} ->
        overloaded(conn)

      # A suffix's size came from a `HEAD` the object no longer matches.
      {:error, :version_mismatch} when attempt == 1 ->
        Budget.release(gateway.instance)
        get(conn, gateway, space, key, 2)

      {:error, :version_mismatch} ->
        Ankusa.Gateway.Http.store_error(conn, :version_mismatch)

      {:error, %Plug.Conn{} = conn} ->
        conn
    end
  end

  # The first byte the first read asks for, the last it may (`nil`: the
  # object's end), and the condition it is held to. A suffix range needs the
  # size first, so its first read is held to the version that size belongs to.
  defp start_of(_conn, _gateway, _space, _key, :all), do: {:ok, 0, nil, []}
  defp start_of(_conn, _gateway, _space, _key, {:span, first, last}), do: {:ok, first, last, []}
  defp start_of(_conn, _gateway, _space, _key, {:from, first}), do: {:ok, first, nil, []}

  defp start_of(conn, gateway, space, key, {:suffix, _n} = parsed) do
    {module, _opts} = space.store
    opts = Config.store_opts(gateway, space)

    case module.head_object(gateway.instance, Spaces.object_key(space, key), opts) do
      {:ok, meta} ->
        case Range.resolve(parsed, meta.size) do
          {first, last} -> {:ok, first, last, [if: {:version, meta.version}]}
          :unsatisfiable -> {:error, unsatisfiable(conn, meta.size)}
        end

      {:error, reason} ->
        {:error, Ankusa.Gateway.Http.store_error(conn, reason)}
    end
  end

  defp first_read(conn, gateway, space, key, parsed, {start, cap, held}) do
    {module, _opts} = space.store
    last = start + space.read_first_bytes - 1
    last = if cap, do: min(cap, last), else: last
    object_key = Spaces.object_key(space, key)

    opts =
      Config.store_opts(gateway, space) ++
        [range: {start, last}, max_bytes: last - start + 1] ++ held

    case module.get_object(gateway.instance, object_key, opts) do
      {:ok, meta, bin} ->
        {:ok, meta, bin}

      {:error, :version_mismatch} ->
        {:error, :version_mismatch}

      {:error, :range_not_satisfiable} ->
        {:error, empty_or_unsatisfiable(conn, gateway, space, key, parsed)}

      {:error, reason} ->
        {:error, Ankusa.Gateway.Http.store_error(conn, reason)}
    end
  end

  # A range read the store cannot satisfy: an empty object asked for whole is
  # an empty `200`; anything else is `416` with the size.
  defp empty_or_unsatisfiable(conn, gateway, space, key, parsed) do
    {module, _opts} = space.store
    opts = Config.store_opts(gateway, space)

    case module.head_object(gateway.instance, Spaces.object_key(space, key), opts) do
      {:ok, %{size: 0} = meta} when parsed == :all ->
        if not_modified?(conn, meta),
          do: not_modified(conn, meta),
          else: conn |> object_headers(meta) |> Plug.Conn.send_resp(200, "")

      {:ok, meta} ->
        unsatisfiable(conn, meta.size)

      {:error, reason} ->
        Ankusa.Gateway.Http.store_error(conn, reason)
    end
  end

  # `first` is what the first read returned, from byte `start`.
  defp respond(conn, {gateway, space, key}, {meta, first, start}, resolved, attempt) do
    {from, to} =
      case resolved do
        :all -> {0, meta.size - 1}
        {from, to} -> {from, to}
      end

    cond do
      not_modified?(conn, meta) ->
        not_modified(conn, meta)

      to - from + 1 > space.max_object_bytes ->
        Http.send_json(conn, 422, %{error: "object_too_large", max_bytes: space.max_object_bytes})

      true ->
        case rest(gateway, space, key, meta, first, start, to) do
          {:ok, bytes} ->
            send_object(conn, meta, resolved, bytes)

          {:error, :version_mismatch} when attempt == 1 ->
            Budget.release(gateway.instance)
            get(conn, gateway, space, key, 2)

          {:error, :version_mismatch} ->
            Ankusa.Gateway.Http.store_error(conn, :version_mismatch)

          {:error, :shed} ->
            overloaded(conn)

          {:error, reason} ->
            Ankusa.Gateway.Http.store_error(conn, reason)
        end
    end
  end

  # The bytes past the first read, up to `to`, held to the first read's version.
  defp rest(gateway, space, key, meta, first, start, to) do
    next = start + byte_size(first)

    if next > to do
      {:ok, binary_part(first, 0, to - start + 1)}
    else
      rest_read(gateway, space, key, meta, first, next, to)
    end
  end

  defp rest_read(gateway, space, key, meta, first, next, to) do
    {module, _opts} = space.store
    size = to - next + 1

    opts =
      Config.store_opts(gateway, space) ++
        [range: {next, to}, if: {:version, meta.version}, max_bytes: size]

    with :ok <- reserve(gateway, size),
         {:ok, _meta, more} <-
           module.get_object(gateway.instance, Spaces.object_key(space, key), opts) do
      {:ok, [first, more]}
    end
  end

  defp reserve(gateway, bytes) do
    case Budget.reserve(gateway.instance, bytes, gateway.objects.max_inflight_bytes) do
      :ok -> :ok
      :shed -> {:error, :shed}
    end
  end

  defp not_modified?(conn, meta) do
    Enum.any?(Plug.Conn.get_req_header(conn, "if-none-match"), fn header ->
      header
      |> String.split(",")
      |> Enum.any?(&(unquote_etag(&1) == meta.version))
    end)
  end

  defp not_modified(conn, meta) do
    conn
    |> Plug.Conn.put_resp_header("etag", etag(meta.version))
    |> Plug.Conn.put_resp_header("cache-control", "private, no-cache")
    |> Plug.Conn.send_resp(304, "")
  end

  defp unsatisfiable(conn, size) do
    conn
    |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
    |> Plug.Conn.put_resp_header("content-range", "bytes */#{size}")
    |> Plug.Conn.send_resp(416, "")
  end

  # Sandboxed and served as an attachment (object_headers/2), never sniffed.
  # sobelow_skip ["XSS.SendResp", "XSS.ContentType"]
  defp send_object(conn, meta, :all, bytes),
    do: conn |> object_headers(meta) |> Plug.Conn.send_resp(200, bytes)

  # sobelow_skip ["XSS.SendResp", "XSS.ContentType"]
  defp send_object(conn, meta, {from, to}, bytes) do
    conn
    |> object_headers(meta)
    |> Plug.Conn.put_resp_header("content-range", "bytes #{from}-#{to}/#{meta.size}")
    |> Plug.Conn.send_resp(206, bytes)
  end

  defp object_headers(conn, meta) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("content-type", meta.content_type || @default_content_type)
      |> Plug.Conn.put_resp_header("etag", etag(meta.version))
      |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
      |> Plug.Conn.put_resp_header("cache-control", "private, no-cache")
      |> Plug.Conn.put_resp_header("x-content-type-options", "nosniff")
      |> Plug.Conn.put_resp_header("content-disposition", "attachment")
      |> Plug.Conn.put_resp_header("content-security-policy", "default-src 'none'; sandbox")
      |> last_modified(meta.last_modified)

    meta.metadata
    |> Enum.reject(fn {name, _value} -> String.starts_with?(name, "ankusa-") end)
    |> Enum.sort()
    |> Enum.reduce(conn, fn {name, value}, conn ->
      Plug.Conn.put_resp_header(conn, "x-ankusa-meta-" <> name, value)
    end)
  end

  defp last_modified(conn, nil), do: conn

  defp last_modified(conn, unix) do
    date = unix |> DateTime.from_unix!() |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")
    Plug.Conn.put_resp_header(conn, "last-modified", date)
  end

  defp etag(version), do: ~s("#{version}")

  # An entity tag as a client sends it (`"v"`, `W/"v"`) to the bare version.
  defp unquote_etag(value),
    do: value |> String.trim() |> String.trim_leading("W/") |> String.trim("\"")

  # ── PUT ─────────────────────────────────────────────────────────────────────

  defp put(conn, gateway, space, key) do
    with {:ok, length} <- content_length(conn, space),
         {:ok, metadata} <- metadata(conn),
         :ok <- reserve(gateway, length),
         {:ok, body, conn} <- body(conn, gateway, length) do
      store_put(conn, gateway, space, key, body, metadata)
    else
      {:error, :shed} -> overloaded(conn)
      {:error, %Plug.Conn{} = conn} -> conn
    end
  end

  defp content_length(conn, space) do
    case Plug.Conn.get_req_header(conn, "content-length") do
      [value] ->
        case Integer.parse(value) do
          {length, ""} when length >= 0 and length <= space.max_object_bytes ->
            {:ok, length}

          {length, ""} when length > space.max_object_bytes ->
            {:error,
             Http.send_json(conn, 413, %{error: "too_large", max_bytes: space.max_object_bytes})}

          _ ->
            {:error, Http.send_json(conn, 400, %{error: "invalid_content_length"})}
        end

      _ ->
        {:error, Http.send_json(conn, 411, %{error: "length_required"})}
    end
  end

  defp metadata(conn) do
    pairs =
      for {"x-ankusa-meta-" <> name, value} <- conn.req_headers, do: {name, value}

    if length(pairs) <= @max_metadata and Enum.all?(pairs, &metadata_pair?/1) do
      {:ok, Map.new(pairs)}
    else
      {:error, Http.send_json(conn, 400, %{error: "invalid_metadata"})}
    end
  end

  defp metadata_pair?({name, value}) do
    Regex.match?(@metadata_name, name) and not String.starts_with?(name, "ankusa-") and
      byte_size(value) <= @max_metadata_value and printable_ascii?(value)
  end

  defp printable_ascii?(value),
    do: for(<<byte <- value>>, reduce: true, do: (ok -> ok and byte in 0x20..0x7E))

  defp body(conn, gateway, length) do
    read_opts = Http.read_opts(Ankusa.config(gateway.instance))

    case Http.read_body_limited(conn, max(length, 1), read_opts) do
      {:ok, body, conn} when byte_size(body) == length ->
        {:ok, body, conn}

      {:ok, _short, conn} ->
        {:error, Http.send_json(conn, 400, %{error: "invalid_content_length"})}

      {:too_large, conn} ->
        {:error, Http.send_json(conn, 400, %{error: "invalid_content_length"})}

      {:error, _reason, conn} ->
        {:error, Http.send_json(conn, 408, %{error: "request_timeout"})}
    end
  end

  defp store_put(conn, gateway, space, key, body, metadata) do
    {module, _opts} = space.store

    opts =
      Config.store_opts(gateway, space) ++
        [content_type: content_type(conn), metadata: metadata] ++ condition(conn)

    case module.put_object(gateway.instance, Spaces.object_key(space, key), body, opts) do
      {:ok, version} ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag(version))
        |> Http.send_json(200, %{
          space: space.name,
          key: key,
          size: byte_size(body),
          version: version
        })

      {:error, reason} when reason in [:already_exists, :version_mismatch] ->
        Http.send_json(conn, 412, %{error: "precondition_failed"})

      {:error, reason} ->
        Ankusa.Gateway.Http.store_error(conn, reason)
    end
  end

  defp content_type(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [value | _] when value != "" -> value
      _ -> @default_content_type
    end
  end

  defp condition(conn) do
    case {Plug.Conn.get_req_header(conn, "if-none-match"),
          Plug.Conn.get_req_header(conn, "if-match")} do
      {["*"], _} -> [if: :absent]
      {_, [version | _]} -> [if: {:version, unquote_etag(version)}]
      _ -> []
    end
  end

  # ── LIST ────────────────────────────────────────────────────────────────────

  defp list_query(conn) do
    params = conn.query_params
    prefix = Map.get(params, "prefix", "")

    with {:prefix, :ok} <- {:prefix, Spaces.validate_prefix(prefix)},
         {:delimiter, {:ok, delimiter}} <- {:delimiter, delimiter(params["delimiter"])},
         {:limit, {:ok, limit}} <- {:limit, limit(params["limit"])},
         {:cursor, {:ok, cursor}} <- {:cursor, cursor(params["cursor"])} do
      {:ok, %{prefix: prefix, delimiter: delimiter, limit: limit, cursor: cursor}}
    else
      {:prefix, _} -> {:error, Http.send_json(conn, 400, %{error: "invalid_key"})}
      {:delimiter, _} -> {:error, Http.send_json(conn, 400, %{error: "invalid_delimiter"})}
      {:limit, _} -> {:error, Http.send_json(conn, 400, %{error: "invalid_limit"})}
      {:cursor, _} -> {:error, Http.send_json(conn, 400, %{error: "invalid_cursor"})}
    end
  end

  defp delimiter(nil), do: {:ok, nil}
  defp delimiter("/"), do: {:ok, "/"}
  defp delimiter(_other), do: :error

  defp limit(nil), do: {:ok, @default_limit}

  defp limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit in 1..1000 -> {:ok, limit}
      _ -> :error
    end
  end

  defp limit(_value), do: :error

  defp cursor(nil), do: {:ok, nil}
  defp cursor(""), do: {:ok, nil}
  defp cursor(value) when is_binary(value) and byte_size(value) <= @max_cursor, do: {:ok, value}
  defp cursor(_value), do: :error

  defp list_page(conn, gateway, space, query) do
    {module, _opts} = space.store

    opts =
      Config.store_opts(gateway, space) ++
        [cursor: query.cursor, limit: query.limit, delimiter: query.delimiter]

    case module.list_page(gateway.instance, space.prefix <> query.prefix, opts) do
      {:ok, page} ->
        objects =
          for %{key: key, size: size} <- page.objects,
              key = strip(key, space.prefix),
              visible?(key),
              do: %{key: key, size: size}

        prefixes =
          for prefix <- page.prefixes,
              prefix = strip(prefix, space.prefix),
              visible?(prefix),
              do: prefix

        Http.send_json(conn, 200, %{objects: objects, prefixes: prefixes, next_cursor: page.next})

      {:error, reason} ->
        Ankusa.Gateway.Http.store_error(conn, reason)
    end
  end

  defp strip(key, ""), do: key
  defp strip(key, prefix), do: String.replace_prefix(key, prefix, "")

  # The ACL and audit trees (and LocalFS's sidecars) never show.
  defp visible?(key),
    do: not Enum.any?(String.split(key, "/"), &String.starts_with?(&1, ".ankusa"))

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp overloaded(conn) do
    conn
    |> retry_after()
    |> Http.send_json(503, %{error: "overloaded"})
  end

  defp retry_after(conn),
    do: Plug.Conn.put_resp_header(conn, "retry-after", Integer.to_string(Enum.random(1..3)))

  defp instance(%Plug.Conn{} = conn),
    do: Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
end
