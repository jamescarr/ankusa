defmodule Ankusa.AsyncApi do
  @moduledoc """
  The AsyncAPI 3.0 document for a running instance: every messaging channel it
  publishes to, built from what is configured right now.

  Served by the admin API at `GET /asyncapi.json` (`application/asyncapi+json`).
  A consumer fetches it to learn which topic, routing key, subject, or channel
  carries which source, and what the message looks like.

  ## What it is built from

  Each source in `Ankusa.SourceStore` contributes one entry per sink that
  implements `c:Ankusa.Sink.describe/2`: the Kafka, RabbitMQ, NATS, and Redis
  sinks. `Ankusa.Sink.Log` and `Ankusa.Sink.Http` have no channel to advertise
  and are left out. When lifecycle events are on (`Ankusa.Lifecycle`), their
  sinks contribute entries too, carrying `LifecycleEventV1`.

  Entries become document parts like so:

    * a **server** per distinct broker (`protocol`, `host`, `pathname`);
    * a **channel** per distinct address on a server. Two sources publishing to
      one Kafka topic share one channel and have one message each, told apart by
      the record key. A sink whose address a configured function computes per
      hook has no address to advertise: it gets a channel of its own with a `null`
      address, and a description saying so;
    * a **message** per source in its channel, whose payload is `SinkMessageV1`
      narrowed to that source (and its tenant, when the route resolver fixes it);
    * an **operation** per channel, `send`: Ankusa is the publisher.

  The document describes the envelope, not the hook: the provider's own body is
  opaque bytes inside `body_base64`.

  Nothing in the document is secret. Descriptions carry no userinfo and no
  credentials (`Ankusa.Sink.Description`).
  """

  alias Ankusa.{Lifecycle, Sink, SourceStore}
  alias Ankusa.AsyncApi.{LifecycleEvent, SinkMessage, SinkMessageHeaders}
  alias AsyncApiSpex.{Channel, Document, Info, Message, Operation, Reference, Server}

  @doc "Build the document for `instance` from its current configuration."
  @spec document(atom()) :: Document.t()
  def document(instance) do
    entries =
      (hook_entries(instance) ++ lifecycle_entries(instance))
      |> Enum.with_index()
      |> Enum.map(fn {entry, index} -> Map.put(entry, :index, index) end)

    server_ids = unique_ids(entries |> Enum.map(&server_key/1) |> Enum.uniq(), &server_base/1)

    channels =
      entries
      |> Enum.group_by(&channel_key/1)
      |> then(fn groups ->
        ids = unique_ids(Map.keys(groups), &channel_base(&1, server_ids))

        for {key, group} <- groups,
            do: build_channel(Map.fetch!(ids, key), key, group, server_ids)
      end)

    %Document{
      id: "urn:ankusa:instance:#{instance}",
      default_content_type: "application/json",
      info: %Info{
        title: "Ankusa",
        version: to_string(Application.spec(:ankusa, :vsn)),
        description:
          "Messaging channels instance #{instance} publishes to. Every message is an " <>
            "Ankusa.Sink.Message v1 envelope; hook bodies are the provider's bytes (base64 " <>
            "inline or claim-checked) and are not described here."
      },
      servers: servers(entries, server_ids),
      channels: Map.new(channels, fn {id, channel, _operation} -> {id, channel} end),
      operations:
        Map.new(channels, fn {id, _channel, operation} -> {"send-" <> id, operation} end)
    }
  end

  # ── entries ───────────────────────────────────────────────────────────────

  # One entry per described sink of every source, in source-id order.
  defp hook_entries(instance) do
    resolver = Ankusa.config(instance).route_resolver

    instance
    |> SourceStore.list()
    |> Enum.sort()
    |> Enum.flat_map(fn source_id ->
      case SourceStore.fetch(instance, source_id) do
        {:ok, source} ->
          describe_all(source.sinks, source_id, tenant_for(resolver, source), false)

        # A miss, or a store that cannot answer right now: nothing to describe.
        _error_or_unavailable ->
          []
      end
    end)
  end

  defp lifecycle_entries(instance) do
    case Lifecycle.source(instance) do
      {:ok, source} -> describe_all(source.sinks, source.id, nil, true)
      :error -> []
    end
  end

  # The path resolver takes the tenant from the source, so every hook of the
  # source carries it. Any other resolver reads the tenant from the URL, per hook.
  defp tenant_for({Ankusa.RouteResolver.Path, _opts}, source), do: source.tenant_id
  defp tenant_for(_resolver, _source), do: nil

  defp describe_all(sinks, source_id, tenant_id, lifecycle?) do
    subject = %{source_id: source_id, tenant_id: tenant_id}

    for {mod, opts} <- sinks,
        description = Sink.describe(mod, subject, opts),
        description != nil do
      %{source_id: source_id, tenant_id: tenant_id, lifecycle?: lifecycle?, d: description}
    end
  end

  # ── servers ───────────────────────────────────────────────────────────────

  defp server_key(%{d: d}), do: {d.protocol, d.host, d.pathname}

  defp server_base({protocol, host, pathname}) do
    [protocol, host, pathname] |> Enum.reject(&is_nil/1) |> Enum.join("-") |> sanitize()
  end

  defp servers(entries, server_ids) do
    entries
    |> Enum.map(&server_key/1)
    |> Enum.uniq()
    |> Map.new(fn {protocol, host, pathname} = key ->
      {Map.fetch!(server_ids, key), %Server{host: host, protocol: protocol, pathname: pathname}}
    end)
  end

  # ── channels ──────────────────────────────────────────────────────────────

  # Entries share a channel when they publish to the same address on the same
  # broker with the same protocol bindings. An address no one knows ahead of time
  # cannot be shared with anything.
  defp channel_key(%{d: %{address: nil}, index: index} = entry),
    do: {:dynamic, server_key(entry), entry.source_id, index}

  defp channel_key(%{d: d} = entry),
    do: {:shared, server_key(entry), d.address, d.channel_bindings}

  defp channel_base({:shared, server_key, address, _bindings}, server_ids),
    do: sanitize("#{Map.fetch!(server_ids, server_key)}-#{address}")

  defp channel_base({:dynamic, server_key, source_id, _index}, server_ids),
    do: sanitize("#{Map.fetch!(server_ids, server_key)}-#{source_id}-dynamic")

  defp build_channel(id, key, entries, server_ids) do
    entries = Enum.sort_by(entries, & &1.index)
    keys = unique_ids(Enum.map(entries, & &1.index), &message_base(entries, &1))

    messages =
      Map.new(entries, fn entry -> {Map.fetch!(keys, entry.index), message(entry)} end)

    {server_key, address, bindings} = channel_parts(key, entries)

    channel = %Channel{
      address: address,
      title: address || "Dynamic address (#{hd(entries).source_id})",
      description: description(entries),
      servers: [%Reference{ref: "#/servers/#{Map.fetch!(server_ids, server_key)}"}],
      messages: messages,
      bindings: nilify(bindings)
    }

    operation = %Operation{
      action: :send,
      channel: %Reference{ref: "#/channels/#{id}"},
      summary: "Ankusa publishes to this channel",
      messages:
        for(
          message_key <- messages |> Map.keys() |> Enum.sort(),
          do: message_ref(id, message_key)
        )
    }

    {id, channel, operation}
  end

  defp channel_parts({:shared, server_key, address, bindings}, _entries),
    do: {server_key, address, bindings}

  defp channel_parts({:dynamic, server_key, _source_id, _index}, [entry]),
    do: {server_key, nil, entry.d.channel_bindings}

  defp message_ref(channel_id, message_key),
    do: %Reference{ref: "#/channels/#{channel_id}/messages/#{message_key}"}

  defp description(entries) do
    {lifecycle, hooks} = Enum.split_with(entries, & &1.lifecycle?)

    [
      if(hooks != [], do: "Hooks from: " <> source_list(hooks)),
      if(lifecycle != [],
        do:
          "Ankusa lifecycle events (CloudEvents 1.0, structured), published from memory: " <>
            "retried, not persisted, not ordered"
      ),
      if(Enum.any?(entries, &(&1.d.address == nil)),
        do: "The address is computed per hook by a configured function and cannot be advertised."
      )
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(". ")
  end

  defp source_list(entries),
    do: entries |> Enum.map(& &1.source_id) |> Enum.uniq() |> Enum.join(", ")

  # ── messages ──────────────────────────────────────────────────────────────

  defp message_base(entries, index) do
    entries |> Enum.find(&(&1.index == index)) |> Map.fetch!(:source_id) |> sanitize()
  end

  defp message(%{lifecycle?: lifecycle?, d: d} = entry) do
    %Message{
      name: entry.source_id,
      title:
        if(lifecycle?, do: "Ankusa lifecycle event", else: "Hook from source #{entry.source_id}"),
      content_type: "application/json",
      payload: %{
        "allOf" => [SinkMessage, %{"type" => "object", "properties" => narrowing(entry)}]
      },
      headers: if(d.ankusa_headers, do: SinkMessageHeaders),
      bindings: nilify(d.message_bindings),
      extensions: if(lifecycle?, do: lifecycle_extension(), else: %{})
    }
  end

  # What is fixed about this source's messages, beyond the envelope's shape.
  defp narrowing(%{source_id: source_id, tenant_id: tenant_id, lifecycle?: lifecycle?}) do
    %{"source_id" => %{"const" => source_id}}
    |> put_if(is_binary(tenant_id), "tenant_id", %{"const" => tenant_id})
    |> put_if(lifecycle?, "content_type", %{"const" => "application/cloudevents+json"})
  end

  defp put_if(map, true, key, value), do: Map.put(map, key, value)
  defp put_if(map, false, _key, _value), do: map

  # The body of a lifecycle message is a CloudEvent inside `body_base64`; the
  # payload schema can only say "base64 string", so the decoded shape is named here.
  defp lifecycle_extension do
    %{
      "x-ankusa-body" => %{
        "contentType" => "application/cloudevents+json",
        "schema" => LifecycleEvent
      }
    }
  end

  # ── ids ───────────────────────────────────────────────────────────────────

  # Component, channel, and message ids are restricted to `[A-Za-z0-9_-]`; every
  # other character becomes `_`.
  defp sanitize(string), do: String.replace(string, ~r/[^A-Za-z0-9_-]/, "_")

  # An id per key: its base, or the base with `_2`, `_3`, … when sanitizing made
  # two keys collide. Keys are taken in base order (then by key) so the same
  # configuration always yields the same ids.
  defp unique_ids(keys, base_fun) do
    keys
    |> Enum.sort_by(&{base_fun.(&1), inspect(&1)})
    |> Enum.reduce({%{}, MapSet.new()}, fn key, {ids, used} ->
      id = first_free(base_fun.(key), used)
      {Map.put(ids, key, id), MapSet.put(used, id)}
    end)
    |> elem(0)
  end

  defp first_free(base, used), do: first_free(base, base, 2, used)

  defp first_free(candidate, base, n, used) do
    if MapSet.member?(used, candidate),
      do: first_free("#{base}_#{n}", base, n + 1, used),
      else: candidate
  end

  defp nilify(map) when map == %{}, do: nil
  defp nilify(map), do: map
end
