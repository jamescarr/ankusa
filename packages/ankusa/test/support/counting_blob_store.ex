defmodule Ankusa.Test.CountingBlobStore do
  @moduledoc """
  A `BlobStore` for tests: `BlobStore.LocalFS` underneath, plus a `{:blob_put,
  key}` message to `:pid` on every `put/4`, so a test can count object writes.

  Opts:

    * `:pid` — receives `{:blob_put, key}` per put
    * `:fail_tenant` — every put under that tenant's claims partition fails
    * `:failures` — an `Agent` holding how many upcoming puts to fail
    * `:put_delay_ms` — sleep before each put, to hold a pack upload open
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.BlobStore.LocalFS

  @impl true
  def put(instance, key, data, opts) do
    if pid = Keyword.get(opts, :pid), do: send(pid, {:blob_put, key})
    if delay = Keyword.get(opts, :put_delay_ms), do: Process.sleep(delay)

    if fail?(key, opts),
      do: {:error, :injected_failure},
      else: LocalFS.put(instance, key, data, [])
  end

  defp fail?(key, opts) do
    tenant = Keyword.get(opts, :fail_tenant)

    cond do
      tenant && String.contains?(key, "tenant=#{tenant}/") ->
        true

      agent = Keyword.get(opts, :failures) ->
        Agent.get_and_update(agent, fn
          n when n > 0 -> {true, n - 1}
          n -> {false, n}
        end)

      true ->
        false
    end
  end

  @impl true
  def get(instance, key, _opts), do: LocalFS.get(instance, key, [])

  @impl true
  def get_range(instance, key, offset, length, _opts),
    do: LocalFS.get_range(instance, key, offset, length, [])

  @impl true
  def delete(instance, key, _opts), do: LocalFS.delete(instance, key, [])

  @impl true
  def list(instance, prefix, _opts), do: LocalFS.list(instance, prefix, [])
end
