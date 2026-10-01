defmodule Ankusa.SDK.Handler do
  @moduledoc """
  What a hook consumer implements: one callback for both transports.

  Return `:ok` only once the hook is durably handled — the same contract
  `docs/integrations.md` ("HTTP handoff") states for any receiver. Return
  `{:error, reason}` for "not handled": `Ankusa.SDK.Receiver` answers it with
  `503`, so Ankusa's dispatcher retries, and a queue consumer should treat it as
  a requeue. A raise, or any other return value, is not rescued: the Receiver
  lets it surface as a `500`, which Ankusa retries the same way.

  ```elixir
  defmodule MyApp.Hooks do
    @behaviour Ankusa.SDK.Handler

    @impl Ankusa.SDK.Handler
    def handle_hook(%Ankusa.SDK.Hook{} = hook, _arg) do
      case MyApp.Store.insert(hook.id, hook.body) do
        :inserted -> :ok
        :duplicate -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end
  ```
  """

  alias Ankusa.SDK.Hook

  @callback handle_hook(hook :: Hook.t(), arg :: term()) :: :ok | {:error, term()}
end
