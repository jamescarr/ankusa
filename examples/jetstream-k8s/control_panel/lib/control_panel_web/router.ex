defmodule ControlPanelWeb.Router do
  use ControlPanelWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {ControlPanelWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  scope "/", ControlPanelWeb do
    get "/health", HealthController, :index
  end

  scope "/", ControlPanelWeb do
    pipe_through :browser

    live "/", DashboardLive
  end
end
