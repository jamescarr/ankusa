if Code.ensure_loaded?(ExUnit.CaseTemplate) do
  defmodule Ankusa.WAL.ConformanceCase do
    @moduledoc """
    The shared `Ankusa.WAL` conformance suite.

    Every adapter must satisfy the same contract, so the contract is tested
    once, here, and each adapter package `use`s this template with its own
    `Ankusa.WAL.Conformance.Adapter` implementation:

        defmodule Ankusa.WAL.RaConformanceTest do
          use Ankusa.WAL.ConformanceCase,
            adapter: Ankusa.WAL.Ra.Conformance
        end

    Options:

      * `:adapter` (**required**) — a module implementing
        `Ankusa.WAL.Conformance.Adapter`.
      * `:config` — extra `Ankusa.Config.new/1` overrides, either a keyword list
        or a 1-arity function of the generated instance name (needed when the
        config itself depends on the instance, e.g. a Ra member list).

    Each test gets a fresh instance name (`:"conf_<unique>"`) and a temp
    `data_dir` under `System.tmp_dir!/0`, so adapters never share state. A test
    module may override the injected `boot/1` and `stop/1` when it has to do
    more than call the adapter (e.g. start peer nodes).

    This module lives in `lib/` but is compiled only where ExUnit is available —
    the same trick `Ecto.Adapters.SQL.Sandbox` uses — so the framework carries
    no runtime dependency on the test framework.
    """

    use ExUnit.CaseTemplate

    alias Ankusa.{Envelope, UUIDv7, WAL}
    alias Ankusa.WAL.LeaseHelpers

    using(opts) do
      adapter = Keyword.fetch!(opts, :adapter)
      config_opt = Keyword.get(opts, :config, [])

      quote do
        use ExUnit.Case, async: false

        import Ankusa.WAL.ConformanceCase,
          only: [envelope: 1, entry: 1, hold!: 3, read_seqs: 3, overrides: 2]

        @adapter unquote(adapter)

        setup do
          instance =
            :"conf_#{System.unique_integer([:positive])}#{Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)}"

          dir =
            Path.join(
              System.tmp_dir!(),
              "ankusa_conf_#{instance}_#{System.unique_integer([:positive])}"
            )

          File.mkdir_p!(dir)

          config =
            Ankusa.Config.new(
              [instance: instance, data_dir: dir] ++ overrides(unquote(config_opt), instance)
            )

          Ankusa.put_config(config)
          boot(config)

          on_exit(fn ->
            stop(config)
            File.rm_rf(dir)
          end)

          %{instance: instance, config: config}
        end

        # Overridable hooks: the default is "just call the adapter".
        def boot(config), do: @adapter.start(config.instance, config)
        def stop(config), do: @adapter.stop(config.instance)
        defoverridable boot: 1, stop: 1

        defp restart(config), do: @adapter.restart(config.instance, config)

        # ── 1 ───────────────────────────────────────────────────────────────

        test "append commits in order and read returns the records byte-exact", %{
          instance: inst
        } do
          a =
            envelope(%{
              body: "one",
              tenant_id: "acme",
              source_id: "github",
              dedup_key: "k1"
            })

          b = envelope(%{body: "two", tenant_id: "other", source_id: "stripe", dedup_key: "k2"})

          assert {:ok, [{:committed, ca}, {:committed, cb}]} =
                   WAL.append(inst, [%{envelope: a}, %{envelope: b}])

          assert ca.seq < cb.seq
          assert {:ok, []} = WAL.append(inst, [])

          assert [ra, rb] = WAL.read(inst, 0, 100)
          assert ra.seq == ca.seq
          assert ra.id == a.id
          assert ra.body == "one"
          assert ra.source_id == "github"
          assert ra.tenant_id == "acme"
          assert rb.seq == cb.seq
          assert rb.body == "two"
          assert rb.source_id == "stripe"
          assert rb.tenant_id == "other"
        end

        # ── 2 ───────────────────────────────────────────────────────────────

        test "read paginates at limit and returns [] past the tail", %{instance: inst} do
          {:ok, committed} =
            WAL.append(inst, for(i <- 1..5, do: %{envelope: envelope(%{body: "b#{i}"})}))

          seqs = Enum.map(committed, fn {:committed, env} -> env.seq end)

          assert read_seqs(inst, 0, 2) == Enum.take(seqs, 2)
          assert read_seqs(inst, Enum.at(seqs, 1), 2) == Enum.slice(seqs, 2, 2)
          assert read_seqs(inst, Enum.at(seqs, 3), 10) == Enum.slice(seqs, 4, 1)
          assert read_seqs(inst, List.last(seqs), 10) == []
        end

        # ── 3 ───────────────────────────────────────────────────────────────

        test "dedup collapses duplicates within a batch, across batches and past truncation",
             %{instance: inst} do
          k = "key-#{System.unique_integer([:positive])}"

          assert {:ok, [{:committed, c1}, {:duplicate, d1}]} =
                   WAL.append(inst, [
                     %{envelope: envelope(%{dedup_key: k})},
                     %{envelope: envelope(%{dedup_key: k})}
                   ])

          assert d1 == c1.seq

          assert {:ok, [{:duplicate, d2}]} =
                   WAL.append(inst, [%{envelope: envelope(%{dedup_key: k})}])

          assert d2 == c1.seq

          # nil keys never collide
          assert {:ok, [{:committed, n1}]} = WAL.append(inst, [%{envelope: envelope(%{})}])
          assert {:ok, [{:committed, n2}]} = WAL.append(inst, [%{envelope: envelope(%{})}])
          assert n1.seq != n2.seq

          # keys are tenant-scoped
          assert {:ok, [{:committed, t1}]} =
                   WAL.append(inst, [%{envelope: envelope(%{dedup_key: k, tenant_id: "t2"})}])

          assert t1.seq != c1.seq

          # a re-send after truncation still returns the original seq
          lease = hold!(inst, :storage, [])
          assert :ok = WAL.truncate_through(inst, c1.seq, lease.token)

          assert {:ok, [{:duplicate, d3}]} =
                   WAL.append(inst, [%{envelope: envelope(%{dedup_key: k})}])

          assert d3 == c1.seq
        end

        # ── 4 ───────────────────────────────────────────────────────────────

        test "read after truncate_through returns exactly the records above it", %{
          instance: inst
        } do
          {:ok, committed} =
            WAL.append(
              inst,
              for(i <- 1..5, do: %{envelope: envelope(%{body: "b#{i}", dedup_key: "t#{i}"})})
            )

          seqs = Enum.map(committed, fn {:committed, env} -> env.seq end)
          lease = hold!(inst, :storage, [])

          assert :ok = WAL.truncate_through(inst, Enum.at(seqs, 2), lease.token)
          assert read_seqs(inst, 0, 100) == Enum.drop(seqs, 3)
        end

        # ── 5 ───────────────────────────────────────────────────────────────

        test "cursors default to 0, persist across a restart and never decrease", %{
          instance: inst,
          config: config
        } do
          assert WAL.get_cursor(inst, :dispatch) == 0
          assert WAL.get_cursor(inst, :compactor) == 0

          dispatch = hold!(inst, :dispatch, [])
          storage = hold!(inst, :storage, [])

          assert :ok = WAL.put_cursor(inst, :dispatch, 42, dispatch.token)
          assert :ok = WAL.put_cursor(inst, :compactor, 7, storage.token)
          assert WAL.get_cursor(inst, :dispatch) == 42

          # monotonic: a lower write is a no-op
          assert :ok = WAL.put_cursor(inst, :dispatch, 5, dispatch.token)
          assert WAL.get_cursor(inst, :dispatch) == 42

          restart(config)

          assert WAL.get_cursor(inst, :dispatch) == 42
          assert WAL.get_cursor(inst, :compactor) == 7
        end

        # ── 6 ───────────────────────────────────────────────────────────────

        test "truncate_through is idempotent and never removes anything above its argument",
             %{instance: inst} do
          {:ok, committed} =
            WAL.append(inst, for(i <- 1..3, do: %{envelope: envelope(%{body: "b#{i}"})}))

          seqs = Enum.map(committed, fn {:committed, env} -> env.seq end)
          lease = hold!(inst, :storage, [])

          assert :ok = WAL.truncate_through(inst, Enum.at(seqs, 1), lease.token)
          assert read_seqs(inst, 0, 100) == [Enum.at(seqs, 2)]

          # again with the same seq: nothing changes
          assert :ok = WAL.truncate_through(inst, Enum.at(seqs, 1), lease.token)
          assert read_seqs(inst, 0, 100) == [Enum.at(seqs, 2)]

          # and with a lower seq: still nothing changes
          assert :ok = WAL.truncate_through(inst, Enum.at(seqs, 0), lease.token)
          assert read_seqs(inst, 0, 100) == [Enum.at(seqs, 2)]
        end

        # ── 7 ───────────────────────────────────────────────────────────────

        test "a cursor write or truncation without a live lease is fenced", %{instance: inst} do
          assert {:error, :fenced} = WAL.put_cursor(inst, :dispatch, 1, 1)
          assert {:error, :fenced} = WAL.put_cursor(inst, :compactor, 1, 1)
          assert {:error, :fenced} = WAL.truncate_through(inst, 1, 1)
          assert WAL.get_cursor(inst, :dispatch) == 0
        end

        # ── 8 ───────────────────────────────────────────────────────────────

        test "lease lifecycle: acquire, renew, contention, expiry, release", %{instance: inst} do
          {:ok, l1} = LeaseHelpers.hold_lease(inst, :dispatch, ttl_ms: 5_000)

          # a second holder is refused while the first is live
          assert {:error, {:held, holder}} =
                   WAL.acquire_lease(inst, :dispatch, "someone-else", 5_000)

          assert holder == l1.holder

          # renew with the holder's own token works
          assert {:ok, renewed} = WAL.renew_lease(inst, l1)
          assert renewed.token == l1.token

          # a bumped token or a different holder is a lost lease
          assert {:error, :lost} = WAL.renew_lease(inst, %{l1 | token: l1.token + 1})
          assert {:error, :lost} = WAL.renew_lease(inst, %{l1 | holder: "impostor"})

          # release, then a new holder acquires with the next token
          assert :ok = WAL.release_lease(inst, l1)
          {:ok, l2} = LeaseHelpers.hold_lease(inst, :dispatch, holder: "second", ttl_ms: 5_000)
          assert l2.token == l1.token + 1

          # a short lease expires on its own and the next holder takes over
          assert :ok = WAL.release_lease(inst, l2)

          {:ok, short} =
            LeaseHelpers.hold_lease(inst, :dispatch, holder: "short", ttl_ms: 200)

          assert {:error, {:held, _}} =
                   WAL.acquire_lease(inst, :dispatch, "next", 5_000)

          Process.sleep(250)

          {:ok, after_expiry} =
            LeaseHelpers.hold_lease(inst, :dispatch, holder: "next", ttl_ms: 5_000)

          assert after_expiry.token == short.token + 1
        end

        # ── 9 ───────────────────────────────────────────────────────────────

        test "a stale token cannot move a cursor or truncate", %{instance: inst} do
          {:ok, [{:committed, first}]} =
            WAL.append(inst, [%{envelope: envelope(%{body: "a"})}])

          {:ok, d1} = LeaseHelpers.hold_lease(inst, :dispatch, [])
          :ok = WAL.release_lease(inst, d1)
          {:ok, d2} = LeaseHelpers.hold_lease(inst, :dispatch, holder: "second")
          assert d2.token == d1.token + 1

          assert :ok = WAL.put_cursor(inst, :dispatch, first.seq, d2.token)
          assert {:error, :fenced} = WAL.put_cursor(inst, :dispatch, first.seq + 10, d1.token)
          assert WAL.get_cursor(inst, :dispatch) == first.seq

          {:ok, s1} = LeaseHelpers.hold_lease(inst, :storage, [])
          :ok = WAL.release_lease(inst, s1)
          {:ok, s2} = LeaseHelpers.hold_lease(inst, :storage, holder: "second")
          assert s2.token == s1.token + 1

          {:ok, [{:committed, second}]} = WAL.append(inst, [%{envelope: envelope(%{body: "b"})}])
          assert {:error, :fenced} = WAL.truncate_through(inst, second.seq, s1.token)
          assert read_seqs(inst, 0, 100) == [first.seq, second.seq]
        end

        # ── 10 ──────────────────────────────────────────────────────────────

        test "a restart reuses no seq and keeps every acked record readable", %{
          instance: inst,
          config: config
        } do
          {:ok, committed} =
            WAL.append(inst, for(i <- 1..3, do: %{envelope: envelope(%{body: "b#{i}"})}))

          acked = Enum.map(committed, fn {:committed, env} -> env end)
          max_seq = acked |> Enum.map(& &1.seq) |> Enum.max()

          restart(config)

          assert {:ok, [{:committed, fresh}]} = WAL.append(inst, [%{envelope: envelope(%{})}])
          assert fresh.seq > max_seq

          readable = WAL.read(inst, 0, 100)
          ids = MapSet.new(readable, & &1.id)
          assert Enum.all?(acked, fn env -> MapSet.member?(ids, env.id) end)
          assert Enum.uniq_by(readable, & &1.seq) == readable
        end

        # ── 11 ──────────────────────────────────────────────────────────────

        test "stats reports the record shape and an exact next_seq after truncation", %{
          instance: inst
        } do
          {:ok, committed} =
            WAL.append(inst, for(i <- 1..3, do: %{envelope: envelope(%{body: "b#{i}"})}))

          last = committed |> Enum.map(fn {:committed, env} -> env.seq end) |> Enum.max()

          stats = WAL.stats(inst)
          assert stats.records == 3
          assert stats.next_seq >= last + 1
          assert is_integer(stats.bytes)
          assert stats.min_seq
          assert stats.max_seq
          assert is_map(stats.cursors)

          lease = hold!(inst, :storage, [])
          assert :ok = WAL.truncate_through(inst, last, lease.token)

          stats = WAL.stats(inst)
          assert stats.records == 0
          # the sequence must not restart: a fresh allocate continues past the
          # seqs already handed out.
          assert stats.next_seq >= last + 1
        end

        # ── 12 ──────────────────────────────────────────────────────────────

        test "8 writers racing one dedup key commit exactly once", %{instance: inst} do
          k = "race-#{System.unique_integer([:positive])}"

          results =
            1..8
            |> Enum.map(fn _ ->
              Task.async(fn -> WAL.append(inst, [%{envelope: envelope(%{dedup_key: k})}]) end)
            end)
            |> Task.await_many(30_000)
            |> Enum.map(fn {:ok, [result]} -> result end)

          committed = for {:committed, env} <- results, do: env
          duplicates = for {:duplicate, seq} <- results, do: seq

          assert length(committed) == 1
          assert length(duplicates) == 7
          [winner] = committed
          assert Enum.all?(duplicates, &(&1 == winner.seq))
          assert length(WAL.read(inst, 0, 100)) == 1
        end

        # ── 13 ──────────────────────────────────────────────────────────────

        test "a cursor-following reader never misses a committed seq under concurrency", %{
          instance: inst
        } do
          advance = fn envelopes, {cursor, seen} ->
            Enum.reduce(envelopes, {cursor, seen}, fn env, {c, s} ->
              {max(c, env.seq), MapSet.put(s, env.seq)}
            end)
          end

          reader =
            Task.async(fn ->
              poll = fn poll, cursor, seen ->
                receive do
                  :stop -> {cursor, seen}
                after
                  0 ->
                    {cursor, seen} = advance.(WAL.read(inst, cursor, 500), {cursor, seen})
                    Process.sleep(1)
                    poll.(poll, cursor, seen)
                end
              end

              poll.(poll, 0, MapSet.new())
            end)

          writers =
            for i <- 1..8 do
              Task.async(fn ->
                name = :"w#{i}"
                {:ok, lease} = LeaseHelpers.hold_lease(inst, name, [])

                try do
                  for _ <- 1..25 do
                    {:ok, [{:committed, env}]} = WAL.append(inst, [%{envelope: envelope(%{})}])
                    :ok = WAL.put_cursor(inst, name, env.seq, lease.token)
                    env.seq
                  end
                after
                  WAL.release_lease(inst, lease)
                end
              end)
            end

          committed = writers |> Task.await_many(60_000) |> List.flatten() |> MapSet.new()
          assert MapSet.size(committed) == 200

          send(reader.pid, :stop)
          {cursor, seen} = Task.await(reader, 30_000)
          {_cursor, seen} = advance.(WAL.read(inst, cursor, 1_000), {cursor, seen})

          assert MapSet.difference(committed, seen) == MapSet.new()
        end
      end
    end

    # ── helpers usable from the injected test module ───────────────────────

    @doc "Build a valid envelope with a fresh id; overrides win."
    @spec envelope(map()) :: Envelope.t()
    def envelope(overrides) do
      base = %Envelope{
        id: UUIDv7.generate(),
        source_id: "src",
        tenant_id: "t1",
        received_at: System.system_time(:millisecond),
        method: "POST",
        path: "/hooks/src",
        headers: [],
        body: "payload",
        size: byte_size("payload")
      }

      struct(base, overrides)
    end

    @doc "Wrap an envelope as a `Ankusa.WAL` entry."
    @spec entry(map()) :: %{envelope: Envelope.t()}
    def entry(overrides), do: %{envelope: envelope(overrides)}

    @doc "`LeaseHelpers.hold_lease/3`, raising if the lease is taken."
    @spec hold!(atom(), atom(), keyword()) :: Ankusa.WAL.lease()
    def hold!(instance, name, opts) do
      {:ok, lease} = LeaseHelpers.hold_lease(instance, name, opts)
      lease
    end

    @doc """
    Resolve the `:config` option: a keyword list is used as-is, a 1-arity
    function is called with the generated instance name (for config that depends
    on it, e.g. a Ra member list naming the cluster).
    """
    @spec overrides(keyword() | (atom() -> keyword()), atom()) :: keyword()
    def overrides(fun, instance) when is_function(fun, 1), do: fun.(instance)
    def overrides(kw, _instance), do: kw

    @doc "The seqs `read/3` returns."
    @spec read_seqs(atom(), non_neg_integer(), pos_integer()) :: [non_neg_integer()]
    def read_seqs(instance, after_seq, limit) do
      instance |> WAL.read(after_seq, limit) |> Enum.map(& &1.seq)
    end
  end
end
