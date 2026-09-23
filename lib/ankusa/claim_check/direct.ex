defmodule Ankusa.ClaimCheck.Direct do
  @moduledoc """
  `Ankusa.ClaimCheck` adapter that calls the instance's `Ankusa.BlobStore`
  in-process. Pure transport — the facade (`Ankusa.ClaimCheck`) already built
  and validated the ticket; this just moves bytes to/from the derived key
  (`Ankusa.ClaimCheck.Ticket.key/1`).

  The right choice for any Ankusa node that already holds blob-store
  credentials, at any fleet size — a shared bucket is already a working
  distributed claim check without an extra network hop.

  opts:

    * `:blob_store` — `{module, opts}` implementing `Ankusa.BlobStore`;
      default: the instance's configured `storage.blob_store`
  """

  @behaviour Ankusa.ClaimCheck

  alias Ankusa.ClaimCheck.Ticket
  alias Ankusa.Config

  @impl true
  def store(instance, %Ticket{} = ticket, data, opts) do
    {mod, blob_opts} = resolve_blob_store(instance, opts)

    case mod.put(instance, Ticket.key(ticket), data, blob_opts) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  @impl true
  def fetch(instance, %Ticket{} = ticket, opts) do
    {mod, blob_opts} = resolve_blob_store(instance, opts)

    case mod.get(instance, Ticket.key(ticket), blob_opts) do
      {:ok, bin} -> {:ok, bin}
      {:error, :not_found} -> {:error, :not_found}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp resolve_blob_store(instance, opts) do
    case Keyword.get(opts, :blob_store) do
      nil ->
        %Config{storage: %{blob_store: bs}} = Ankusa.config(instance)
        bs

      bs ->
        bs
    end
  end
end
