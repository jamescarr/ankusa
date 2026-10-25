defmodule AnkusaServer.Tracing do
  @moduledoc """
  Starts the OpenTelemetry SDK and the OTLP exporter when the config file says
  `tracing.enabled: true`.

  Core carries only `opentelemetry_api`, so without an SDK every span call is a
  no-op and a hook's `traceparent` rides through verbatim (`Ankusa.Trace`). This
  module is the switch: it writes the SDK's application environment from the
  `tracing` section and starts the two applications, before the instance, so the
  first request is already traced. `tracing.enabled: false` (the default) starts
  nothing: the SDK and the exporter's own dependencies are in the image
  (`runtime: false`, `:load` in the release) but never run.

  | Key | Becomes |
  | --- | --- |
  | `otlp_endpoint` | the exporter's `otlp_endpoint`; protocol `http_protobuf`, so spans go to `<endpoint>/v1/traces` |
  | `sample_ratio` | `parent_based` with a `trace_id_ratio_based` root: a request that carries a sampled `traceparent` is always traced, one that does not is traced with this probability |

  The SDK's own `OTEL_*` environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
  `OTEL_TRACES_SAMPLER`, `OTEL_SERVICE_NAME`, ...) take precedence over what
  this module writes, as they do in every OpenTelemetry SDK. The collector's
  credentials are the standard `OTEL_EXPORTER_OTLP_HEADERS` (and
  `OTEL_EXPORTER_OTLP_TRACES_HEADERS`), never a key of the config file. The
  resource is `service.name = "ankusa"` and `service.version` the image's.
  """

  @doc """
  Start tracing for a loaded `tracing` section (`t:AnkusaServer.Config.tracing/0`).
  A failure to start is returned, and `AnkusaServer.Application` logs it and
  carries on untraced: tracing is observability, and must never keep the ingest
  tier from booting.
  """
  @spec start(AnkusaServer.Config.tracing()) :: :ok | {:error, term()}
  def start(%{enabled: false}), do: :ok

  def start(%{enabled: true} = tracing) do
    put_env(:opentelemetry_exporter, exporter_env(tracing))
    put_env(:opentelemetry, sdk_env(tracing))

    # The exporter first: the SDK's processor initializes it as it starts.
    with {:ok, _} <- Application.ensure_all_started(:opentelemetry_exporter),
         {:ok, _} <- Application.ensure_all_started(:opentelemetry) do
      :ok
    end
  end

  @doc """
  The `:opentelemetry` application environment for a `tracing` section.
  """
  @spec sdk_env(AnkusaServer.Config.tracing()) :: keyword()
  def sdk_env(%{sample_ratio: ratio}) do
    [
      sampler: {:parent_based, %{root: {:trace_id_ratio_based, ratio}}},
      processors: [{:otel_batch_processor, %{}}],
      traces_exporter: {:opentelemetry_exporter, %{}},
      resource: %{service: %{name: "ankusa", version: AnkusaServer.Application.version()}}
    ]
  end

  @doc """
  The `:opentelemetry_exporter` application environment for a `tracing` section.
  """
  @spec exporter_env(AnkusaServer.Config.tracing()) :: keyword()
  def exporter_env(%{otlp_endpoint: endpoint}) do
    [otlp_protocol: :http_protobuf, otlp_endpoint: endpoint]
  end

  defp put_env(app, env),
    do: Enum.each(env, fn {key, value} -> Application.put_env(app, key, value) end)
end
