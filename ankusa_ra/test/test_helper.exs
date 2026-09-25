# The Ra cluster and fault suites start real `:peer` VMs and talk to them over
# Erlang distribution. That is not available everywhere — a runner without
# `epmd`, a sandbox that blocks loopback distribution — so it is probed once here
# and the suites that need it are excluded with a loud message instead of hanging
# until their timeouts.
#
# The probe starts its peer exactly the way `Ankusa.WAL.ClusterCase` does,
# cookie included: a probe that fails for a reason the suites would not hit
# excludes them on a host where they would have run.
#
# The single-node conformance suite needs none of this: it runs everywhere.
probe = fn ->
  try do
    {:ok, _pid} = Node.start(:ankusa_ra_test, :shortnames)
    Node.set_cookie(:ankusa_ra_test)

    config = Map.put(Ankusa.WAL.ClusterCase.peer_config(:ankusa_ra_probe), :wait_boot, 3_000)

    case :peer.start_link(config) do
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
    if System.get_env("ANKUSA_REQUIRE_DIST") == "1" do
      raise "Erlang distribution unavailable; :dist suites would be skipped (#{inspect(reason)})"
    end

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
