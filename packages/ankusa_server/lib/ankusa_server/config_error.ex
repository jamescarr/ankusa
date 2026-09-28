defmodule AnkusaServer.ConfigError do
  @moduledoc """
  A config file that cannot be turned into a running instance: a missing file
  named by `ANKUSA_CONFIG`, a YAML syntax error, an unset `${VAR}`, an unknown
  key, a wrong type, or a combination core itself rejects.

  This is deliberately a single exception type with a human-readable `message`
  rather than a struct per failure. The message is the operator-facing artifact —
  `AnkusaServer.Config.load_or_halt!/0` prints it verbatim on boot and for
  `check-config` / `print-config` — so it must always name the offending dotted
  path (`sources.stripe.verify.secret`) and what was wrong with it.
  """

  defexception [:message]
end
