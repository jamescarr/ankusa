# The Ra cluster and fault suites start real `:peer` VMs and talk to them over
# Erlang distribution. That is not available everywhere — a sandbox that blocks
# loopback distribution, a runner without `epmd` — so it is probed once here and
# the suites that need it are excluded with a loud message instead of hanging
# until their timeouts.
#
# The single-node conformance suite needs none of this: it runs everywhere.
probe = fn ->
  {:ok, _pid} = Node.start(:ankusa_ra_test, :shortnames)
  Node.set_cookie(:ankusa_ra_test)

  try do
    case :peer.start_link(%{name: :ankusa_ra_probe, wait_boot: 3_000}) do
      {:ok, pid, _name} ->
        :peer.stop(pid)
        :ok

      other ->
        {:error, other}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end

case probe.() do
  :ok ->
    ExUnit.start()

  {:error, reason} ->
    IO.puts(
      :stderr,
      """
      [ankusa_ra] Erlang distribution between nodes does not work here
        (#{inspect(reason)}), so the multi-node cluster and fault suites are
        excluded. The single-node conformance suite and the model-based property
        suite still run. Run this package on a host with working distribution
        (or CI) to exercise the multi-node paths.
      """
    )

    ExUnit.start(exclude: [:dist])
end
