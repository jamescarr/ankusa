defmodule Ankusa.Redis.Options do
  @moduledoc false
  # Redix start options for a connection URL, with the URL's password replaced
  # by Redix's `{m, f, a}` form: a child spec and Redix's own state then hold the
  # MFA, never the password (Redix has no `format_status`).

  @spec start_opts(String.t(), {module(), atom(), [term()]}) :: keyword()
  def start_opts(url, password_mfa) do
    opts = Redix.URI.to_start_options(url)

    if Keyword.has_key?(opts, :password),
      do: Keyword.put(opts, :password, password_mfa),
      else: opts
  end

  @spec password(String.t()) :: String.t() | nil
  def password(url), do: url |> Redix.URI.to_start_options() |> Keyword.get(:password)
end
