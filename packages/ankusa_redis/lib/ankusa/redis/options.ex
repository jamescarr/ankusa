defmodule Ankusa.Redis.Options do
  @moduledoc false
  # Redix start options for a connection URL, with the URL's password replaced
  # by Redix's `{m, f, a}` form: a child spec and Redix's own state then hold the
  # MFA, never the password (Redix has no `format_status`).

  @default_timeout_ms 5_000
  @default_load_timeout_ms 300_000

  @spec start_opts(String.t(), {module(), atom(), [term()]}) :: keyword()
  def start_opts(url, password_mfa) do
    opts = Redix.URI.to_start_options(url)

    if Keyword.has_key?(opts, :password),
      do: Keyword.put(opts, :password, password_mfa),
      else: opts
  end

  @spec password(String.t()) :: String.t() | nil
  def password(url), do: url |> Redix.URI.to_start_options() |> Keyword.get(:password)

  # `{timeout_ms, load_timeout_ms}` of a store's options, defaults filled in
  # (`label` names the config section in the error). A load is paged (`HSCAN`,
  # `Ankusa.Redis.Scan`), so `timeout_ms` bounds each command, a page included,
  # and `load_timeout_ms` the whole walk: a million entries load without any
  # one command near the short timeout, and a Redis that stalls mid-load still
  # ends it.
  @spec timeouts(keyword(), String.t()) :: {pos_integer(), pos_integer()}
  def timeouts(store_opts, label) do
    timeout_ms = Keyword.get(store_opts, :timeout_ms, @default_timeout_ms)
    load_timeout_ms = Keyword.get(store_opts, :load_timeout_ms, @default_load_timeout_ms)

    for {key, value} <- [timeout_ms: timeout_ms, load_timeout_ms: load_timeout_ms],
        not (is_integer(value) and value >= 1) do
      raise ArgumentError, "#{label} #{key} must be a positive integer, got #{inspect(value)}"
    end

    {timeout_ms, load_timeout_ms}
  end
end
