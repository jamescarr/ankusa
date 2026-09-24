defmodule AnkusaServer.ConfigError do
  @moduledoc """
  A config file that cannot be turned into a running instance: a missing file
  named by `ANKUSA_CONFIG`, a YAML syntax error, an unset `${VAR}`, an unknown
  key, a wrong type, or a combination core itself rejects.

  This is deliberately a single exception type with a human-readable `message`
  rather than a struct per failure. The message is the operator-facing artifact —
  it is printed verbatim by `check-config`, by the container entrypoint, and by
  the application on boot — so it must always name the offending dotted path
  (`sources.stripe.verify.secret`) and what was wrong with it.
  """

  defexception [:message]
end
