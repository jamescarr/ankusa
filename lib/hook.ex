defmodule Hook do
  @moduledoc """
  A loosely coupled, high-throughput webhook ingestion framework.

  ## Core invariant

  Never return `2xx` until the hook is durably stored. The edge acks only after
  the group-commit batcher's WAL commit returns.

  ## Layers (all pluggable behaviours)

    * `Hook.WAL`         — durable, ordered log with fast acks and truncation.
    * `Hook.Verifier`    — signature/timestamp checks (Standard Webhooks, Stripe…).
    * `Hook.DedupKey`    — extract the provider event id for idempotency.
    * `Hook.SourceStore` — per-source config, secrets, and failure policy.
    * `Hook.Sink`        — what happens to a delivered hook.
    * `Hook.RetryPolicy` — dispatch backoff and give-up rules.
    * `Hook.BlobStore` / `Hook.Codec` — long-term segment storage.

  Every process is instance-scoped through `Hook.Registry`, so two independent
  instances can run in one VM. Read-mostly config lives in `:persistent_term`.
  """

  alias Hook.Config

  @doc "Register/lookup name for an instance-scoped process."
  @spec via(atom(), term()) :: {:via, Registry, {module(), {atom(), term()}}}
  def via(instance, key), do: {:via, Registry, {Hook.Registry, {instance, key}}}

  @doc "Look up the running pid for an instance-scoped process, if any."
  @spec whereis(atom(), term()) :: pid() | nil
  def whereis(instance, key) do
    case Registry.lookup(Hook.Registry, {instance, key}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc "Store the instance config in `:persistent_term` for read-mostly access."
  @spec put_config(Config.t()) :: :ok
  def put_config(%Config{instance: instance} = config) do
    :persistent_term.put({__MODULE__, :config, instance}, config)
  end

  @doc "Fetch the instance config."
  @spec config(atom()) :: Config.t()
  def config(instance \\ :default) do
    :persistent_term.get({__MODULE__, :config, instance})
  end
end
