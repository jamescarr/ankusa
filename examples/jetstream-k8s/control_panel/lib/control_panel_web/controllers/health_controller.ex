defmodule ControlPanelWeb.HealthController do
  @moduledoc "Kubernetes readiness/liveness probe target, and run.sh's smoke check."
  use ControlPanelWeb, :controller

  def index(conn, _params), do: json(conn, %{status: "ok"})
end
