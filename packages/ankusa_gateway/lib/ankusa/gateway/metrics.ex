defmodule Ankusa.Gateway.Metrics do
  @moduledoc """
  The gateway's Prometheus series, for core's `metrics.extra`:

      Ankusa.Config.new(metrics: %{enabled: true, extra: [Ankusa.Gateway.Metrics]})

    * `ankusa_gateway_requests_total{instance,api,status}` — authorized
      requests answered, by API and status.
    * `ankusa_gateway_request_duration_seconds{instance,api}` — their duration.
    * `ankusa_gateway_authn_total{instance,authenticator,result,cached}` —
      authentications, by the authenticator that decided.
    * `ankusa_gateway_authz_decisions_total{instance,policy,action,decision,cached}`
      and `ankusa_gateway_authz_duration_seconds{instance,policy}` — policy
      decisions.
    * `ankusa_gateway_calls_total{instance,caller,result}` and
      `ankusa_gateway_call_duration_seconds{instance,caller}` — runs of each
      remote backend's `Ankusa.Gateway.Caller`; `ankusa_gateway_caller_limit`,
      `_inflight`, `_queued` its state, and
      `ankusa_gateway_caller_breaker_transitions_total{instance,caller,state}`.
    * `ankusa_gateway_cache_entries` and `ankusa_gateway_cache_bytes`
      `{instance,cache}` — the `authn`, `authz` and `acl` caches.
    * `ankusa_gateway_objects_inflight_bytes{instance}` — object bytes the
      objects API holds in memory now.
    * `ankusa_gateway_shares_total{instance,action,result}` — sharing
      mutations (`grant_create`, `grant_revoke`, `link_create`, and
      `acl_cleanup` when a deleted object's ACL document could not be removed).

  Every label value is fixed or operator configuration: nothing a caller sends
  (a tenant, a claim id, a method) is a label.
  """

  import Telemetry.Metrics

  # The claims API answers at up to `claim_check.read_deadline_ms` (5 s by
  # default): the tail this histogram exists for sits past 2.5 s.
  @seconds_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
  # A policy decision is microseconds from the cache, milliseconds from a
  # remote backend.
  @authz_buckets [
    0.00005,
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1
  ]

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
      ),
      counter(
        "ankusa.gateway.authn.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :authn],
          tags: [:instance, :authenticator, :result, :cached]
        )
      ),
      counter(
        "ankusa.gateway.authz.decisions.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :authz],
          tags: [:instance, :policy, :action, :decision, :cached]
        )
      ),
      distribution(
        "ankusa.gateway.authz.duration.seconds",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :authz],
          measurement: &Ankusa.Metrics.duration_seconds/1,
          unit: :second,
          tags: [:instance, :policy],
          reporter_options: [buckets: @authz_buckets]
        )
      ),
      counter(
        "ankusa.gateway.calls.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :call],
          tags: [:instance, :caller, :result]
        )
      ),
      distribution(
        "ankusa.gateway.call.duration.seconds",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :call],
          measurement: &Ankusa.Metrics.duration_seconds/1,
          unit: :second,
          tags: [:instance, :caller],
          reporter_options: [buckets: @seconds_buckets]
        )
      ),
      counter(
        "ankusa.gateway.shares.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :share],
          tags: [:instance, :action, :result]
        )
      ),
      counter(
        "ankusa.gateway.caller.breaker.transitions.total",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :caller_breaker],
          tags: [:instance, :caller, :state]
        )
      )
    ] ++
      caller_gauges(instance) ++
      cache_gauges(instance) ++
      [
        last_value(
          "ankusa.gateway.objects.inflight.bytes",
          Ankusa.Metrics.scope(instance,
            event_name: [:ankusa, :gateway, :objects_state],
            measurement: :inflight_bytes,
            tags: [:instance]
          )
        )
      ]
  end

  defp caller_gauges(instance) do
    for measurement <- [:limit, :inflight, :queued] do
      last_value(
        "ankusa.gateway.caller.#{measurement}",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :caller_state],
          measurement: measurement,
          tags: [:instance, :caller]
        )
      )
    end
  end

  defp cache_gauges(instance) do
    for measurement <- [:entries, :bytes] do
      last_value(
        "ankusa.gateway.cache.#{measurement}",
        Ankusa.Metrics.scope(instance,
          event_name: [:ankusa, :gateway, :cache_state],
          measurement: measurement,
          tags: [:instance, :cache]
        )
      )
    end
  end
end
