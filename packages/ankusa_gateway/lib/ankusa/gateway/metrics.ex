defmodule Ankusa.Gateway.Metrics do
  @moduledoc """
  The gateway's Prometheus series, for core's `metrics.extra`:

      Ankusa.Config.new(metrics: %{enabled: true, extra: [Ankusa.Gateway.Metrics]})

    * `ankusa_gateway_requests_total{instance,api,status}` — authorized
      requests answered, by API and status.
    * `ankusa_gateway_request_duration_seconds{instance,api}` — their duration.

  Every label value is fixed or operator configuration: nothing a caller sends
  (a tenant, a claim id, a method) is a label.
  """

  import Telemetry.Metrics

  # The claims API answers at up to `claim_check.read_deadline_ms` (5 s by
  # default): the tail this histogram exists for sits past 2.5 s.
  @seconds_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

  @spec metrics(atom()) :: [Telemetry.Metrics.t()]
  def metrics(instance) do
    [
      counter(
        "ankusa.gateway.requests.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :request],
          tags: [:instance, :api, :status]
        )
      ),
      distribution(
        "ankusa.gateway.request.duration.seconds",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :request],
          measurement: &Ankusa.Metrics.duration_seconds/1,
          unit: :second,
          tags: [:instance, :api],
          reporter_options: [buckets: @seconds_buckets]
        )
      )
    ]
  end
end
