defmodule Ankusa.Gateway.Cache.Store do
  @moduledoc false
  # The Nebulex cache behind `Ankusa.Gateway.Cache`: one module, started once
  # per cache name and instance, always addressed by pid.
  use Nebulex.Cache, otp_app: :ankusa_gateway, adapter: Nebulex.Adapters.Local
end
