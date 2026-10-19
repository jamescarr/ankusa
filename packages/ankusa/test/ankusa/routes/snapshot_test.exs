defmodule Ankusa.Routes.SnapshotTest do
  @moduledoc """
  The snapshot's indexes, proven against what they replaced: the literal-prefix
  index must answer exactly as a walk over every pattern in priority order
  does (the reference below is that walk), whatever mix of patterns, methods,
  disabled routes and prefixes longer than the index's cap the table holds, and
  however it got there (published whole, or edited one route at a time).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Routes
  alias Ankusa.Routes.{Matcher, Snapshot}

  @methods ["POST", "PUT", "GET"]
  @words ~w(hooks a b c d e f g h i j k l stripe github x)

  # The old scan: every route, most specific first, the first enabled match
  # that serves the method wins; a path that matched only other methods is
  # `:method`.
  defp reference(instance, meta, method, segments) do
    routes = Snapshot.routes(instance, meta)

    routes
    |> Enum.map(fn route ->
      compiled = Matcher.compile(route)
      {literals, wildcard?} = Matcher.specificity(compiled)
      {{-literals, if(wildcard?, do: 1, else: 0), route.id}, route, compiled}
    end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:reject, :no_route}, fn {_key, route, compiled}, acc ->
      cond do
        not Matcher.match?(compiled, segments) -> {:cont, acc}
        not route.enabled -> {:cont, acc}
        method in route.methods -> {:halt, {:match, route.id}}
        true -> {:cont, {:reject, :method}}
      end
    end)
  end

  # A deterministic pattern generator: 0..11 segments, each a literal, a param
  # or (last only) a wildcard, so some literal runs are longer than the cap.
  defp random_path(seed_state) do
    {length, seed_state} = :rand.uniform_s(12, seed_state)
    {segments, seed_state} = random_segments(length - 1, seed_state, [])
    {last, seed_state} = random_last(seed_state)
    {"/" <> Enum.join(segments ++ [last], "/"), seed_state}
  end

  defp random_segments(0, seed_state, acc), do: {Enum.reverse(acc), seed_state}

  defp random_segments(count, seed_state, acc) do
    {pick, seed_state} = :rand.uniform_s(10, seed_state)

    {segment, seed_state} =
      if pick <= 2 do
        {":p#{pick}", seed_state}
      else
        {index, seed_state} = :rand.uniform_s(length(@words), seed_state)
        {Enum.at(@words, index - 1), seed_state}
      end

    random_segments(count - 1, seed_state, [segment | acc])
  end

  defp random_last(seed_state) do
    {pick, seed_state} = :rand.uniform_s(10, seed_state)

    cond do
      pick == 1 ->
        {"*", seed_state}

      pick == 2 ->
        {":id", seed_state}

      true ->
        {index, seed_state} = :rand.uniform_s(length(@words), seed_state)
        {Enum.at(@words, index - 1), seed_state}
    end
  end

  defp random_methods(seed_state) do
    {mask, seed_state} = :rand.uniform_s(7, seed_state)
    methods = for {m, bit} <- Enum.zip(@methods, [1, 2, 4]), Bitwise.band(mask, bit) != 0, do: m
    {methods, seed_state}
  end

  # A request path that a pattern could match: its literals kept, its params
  # and wildcard filled in.
  defp request_for(pattern, seed_state) do
    segments =
      pattern
      |> String.split("/", trim: true)
      |> Enum.flat_map(fn
        "*" -> ["tail", "more"]
        ":" <> _param -> ["value"]
        literal -> [literal]
      end)

    {extra, seed_state} = :rand.uniform_s(3, seed_state)
    {if(extra == 1, do: segments ++ ["extra"], else: segments), seed_state}
  end

  defp populate(instance, count, seed_state) do
    Enum.reduce(1..count, {[], seed_state}, fn n, {requests, seed_state} ->
      {path, seed_state} = random_path(seed_state)
      {methods, seed_state} = random_methods(seed_state)
      {enabled, seed_state} = :rand.uniform_s(5, seed_state)

      attrs = %{
        "id" => "r#{n}",
        "path" => path,
        "methods" => methods,
        "enabled" => enabled != 1
      }

      # A collision with an enabled route is a refusal; the table just does not
      # get that route, which is as good a table for this test.
      _ = Routes.create(instance, attrs)
      {request, seed_state} = request_for(path, seed_state)
      {[request | requests], seed_state}
    end)
  end

  defp assert_same(instance, requests) do
    meta = Routes.meta(instance)

    for segments <- requests, method <- @methods do
      assert Snapshot.scan(instance, meta, method, segments) ==
               reference(instance, meta, method, segments),
             "#{method} /#{Enum.join(segments, "/")}"
    end
  end

  describe "the literal-prefix index" do
    test "answers as a walk over every pattern does, for routes written one at a time" do
      config = start_routes(max_routes: 5_000)
      {requests, _seed} = populate(config.instance, 400, :rand.seed_s(:exsss, {1, 2, 3}))

      assert Routes.meta(config.instance).count > 50
      assert_same(config.instance, requests)
    end

    test "answers the same after the table is published whole, and after edits and deletes" do
      config = start_routes(max_routes: 5_000)
      instance = config.instance
      {requests, seed_state} = populate(instance, 300, :rand.seed_s(:exsss, {4, 5, 6}))

      # A Redis reload: the same routes, republished as a new generation.
      store = Ankusa.whereis(instance, :routes_store)

      :sys.replace_state(store, fn state ->
        :ok = Snapshot.publish(state)
        state
      end)

      assert_same(instance, requests)

      # Edits and deletes in place: toggle, rewrite methods, remove.
      ids = instance |> Routes.meta() |> then(&Snapshot.routes(instance, &1)) |> Enum.map(& &1.id)

      Enum.reduce(Enum.take_every(ids, 3), seed_state, fn id, seed_state ->
        {methods, seed_state} = random_methods(seed_state)
        {pick, seed_state} = :rand.uniform_s(3, seed_state)

        case pick do
          1 -> Routes.delete(instance, id)
          2 -> Routes.update(instance, id, %{"methods" => methods})
          3 -> Routes.update(instance, id, %{"enabled" => false})
        end

        seed_state
      end)

      assert_same(instance, requests)
    end

    test "a miss with the cache out of the picture is a walk of the request's own buckets" do
      # 20 000 routes under 4 000 first segments (seeded: an API create is an
      # audited, synced write): a miss may only look at the handful that
      # share the request's leading literals.
      seed =
        for n <- 1..20_000,
            do: %{"id" => "r#{n}", "path" => "/p#{rem(n, 4_000)}/#{n}/:id"}

      config = start_routes(max_routes: 50_000, seed: seed)
      instance = config.instance

      meta = Routes.meta(instance)

      assert Snapshot.scan(instance, meta, "POST", ["p17", "17", "x"]) == {:match, "r17"}
      assert Snapshot.scan(instance, meta, "POST", ["p17", "nope", "x"]) == {:reject, :no_route}
      assert Snapshot.scan(instance, meta, "POST", ["zzz", "1", "x"]) == {:reject, :no_route}
      assert Snapshot.scan(instance, meta, "GET", ["p17", "17", "x"]) == {:reject, :method}

      {micros, _} =
        :timer.tc(fn ->
          for n <- 1..2_000, do: Snapshot.scan(instance, meta, "POST", ["p#{n}", "miss", "x"])
        end)

      # A walk of the whole table is ~20 000 lookups per miss (milliseconds);
      # the index is a few. 2 000 misses stay far under what one full walk costs.
      assert micros / 2_000 < 500
    end

    test "a literal run longer than the index's cap is still found and still ordered" do
      config = start_routes(max_routes: 100)
      instance = config.instance
      long = Enum.map_join(1..11, "/", &"s#{&1}")

      assert {:ok, _} = Routes.create(instance, %{"id" => "deep", "path" => "/#{long}/:x"})
      assert {:ok, _} = Routes.create(instance, %{"id" => "wide", "path" => "/s1/*"})
      assert {:ok, _} = Routes.create(instance, %{"id" => "mid", "path" => "/:a/s2/:b"})

      meta = Routes.meta(instance)
      deep = Enum.map(1..11, &"s#{&1}") ++ ["tail"]

      assert Snapshot.scan(instance, meta, "POST", deep) == {:match, "deep"}
      assert Snapshot.scan(instance, meta, "POST", ["s1", "s2", "z"]) == {:match, "mid"}
      assert Snapshot.scan(instance, meta, "POST", ["s1", "q", "z"]) == {:match, "wide"}
      assert meta.max_prefix == 8
    end
  end

  describe "the overlap query" do
    defp ids(routes), do: routes |> Enum.map(& &1.id) |> Enum.sort()

    defp candidate(id, path, tenant \\ "default") do
      {:ok, route} =
        Ankusa.Routes.Route.from_attrs(%{"id" => id, "path" => path, "tenant_id" => tenant})

      route
    end

    test "names every pattern a candidate shares a request path with, and who owns each" do
      seed = [
        %{"id" => "acme-x", "path" => "/hooks/acme/:x", "tenant_id" => "acme"},
        %{"id" => "acme-y", "path" => "/hooks/acme/y", "tenant_id" => "acme", "enabled" => false},
        %{"id" => "pay", "path" => "/hooks/:t/pay", "tenant_id" => "billing"},
        %{"id" => "all", "path" => "/hooks/*", "tenant_id" => "ops"},
        %{"id" => "other", "path" => "/other/x", "tenant_id" => "acme"},
        %{"id" => "deep", "path" => "/hooks/acme/pay/receipt", "tenant_id" => "acme"}
      ]

      config = start_routes(max_routes: 100, seed: seed)
      instance = config.instance
      meta = Routes.meta(instance)

      overlaps = Snapshot.overlaps(instance, meta, candidate("new", "/hooks/acme/pay", "globex"))
      assert ids(overlaps) == ["acme-x", "all", "pay"]

      assert overlaps |> Map.new(&{&1.id, &1.tenant_id}) == %{
               "acme-x" => "acme",
               "all" => "ops",
               "pay" => "billing"
             }

      # A candidate under a prefix reaches the routes beneath it, disabled ones too.
      assert ids(Snapshot.overlaps(instance, meta, candidate("c", "/hooks/acme/*"))) ==
               ["acme-x", "acme-y", "all", "deep", "pay"]

      assert ids(Snapshot.overlaps(instance, meta, candidate("c", "/other/:z"))) == ["other"]
      assert Snapshot.overlaps(instance, meta, candidate("c", "/nothing/here")) == []

      # A leading parameter has no literal to index under: it reaches everything
      # it can match.
      assert ids(Snapshot.overlaps(instance, meta, candidate("c", "/:a/x"))) ==
               ["all", "other"]

      # A route is not its own overlap, and `limit` stops the walk.
      assert "acme-x" not in ids(
               Snapshot.overlaps(instance, meta, candidate("acme-x", "/hooks/acme/:x"))
             )

      assert length(Snapshot.overlaps(instance, meta, candidate("c", "/hooks/acme/*"), 2)) == 2
    end

    test "agrees with comparing the candidate to every route in a random table" do
      config = start_routes(max_routes: 5_000)
      instance = config.instance
      {_requests, _seed} = populate(instance, 300, :rand.seed_s(:exsss, {7, 8, 9}))
      meta = Routes.meta(instance)
      routes = Snapshot.routes(instance, meta)

      for route <- Enum.take_every(routes, 3) do
        expected =
          for other <- routes,
              other.id != route.id,
              Matcher.overlap?(Matcher.compile(route), Matcher.compile(other)),
              do: other.id

        assert ids(Snapshot.overlaps(instance, meta, route)) == Enum.sort(expected), route.path
      end
    end
  end

  describe "the conflict index" do
    test "finds the enabled holder of a pattern and method in one lookup" do
      config = start_routes(max_routes: 100)
      instance = config.instance

      assert {:ok, _} =
               Routes.create(instance, %{"id" => "a", "path" => "/h/:x", "methods" => ["POST"]})

      assert {:ok, _} =
               Routes.create(instance, %{"id" => "off", "path" => "/h/:y", "enabled" => false})

      # Same compiled pattern (`:x` and `:y` are both a param), a method in common.
      assert {:error, {:conflict, "a"}} =
               Routes.create(instance, %{"id" => "b", "path" => "/h/:y", "methods" => ["POST"]})

      # A disjoint method, a different pattern, or a disabled route do not collide.
      assert {:ok, _} =
               Routes.create(instance, %{"id" => "c", "path" => "/h/:y", "methods" => ["PUT"]})

      assert {:ok, _} = Routes.create(instance, %{"id" => "d", "path" => "/h/:y/z"})

      assert {:ok, _} =
               Routes.create(instance, %{"id" => "e", "path" => "/h/:y", "enabled" => false})

      # Deleting the holder frees the pattern; re-enabling a disabled route
      # takes it again.
      assert :ok = Routes.delete(instance, "a")

      assert {:ok, _} =
               Routes.create(instance, %{"id" => "f", "path" => "/h/:q", "methods" => ["POST"]})

      assert {:error, {:conflict, "f"}} = Routes.update(instance, "e", %{"enabled" => true})
    end
  end

  describe "listing" do
    test "pages in id order from a cursor, without reading the rest of the table" do
      seed =
        for n <- 1..2_000,
            do: %{"id" => "r" <> String.pad_leading("#{n}", 5, "0"), "path" => "/l/#{n}"}

      config = start_routes(max_routes: 10_000, seed: seed)
      instance = config.instance

      assert {:ok, %{routes: first, next_cursor: cursor}} = Routes.list(instance, limit: 3)
      assert Enum.map(first, & &1.id) == ["r00001", "r00002", "r00003"]
      assert cursor == "r00003"

      assert {:ok, %{routes: page, next_cursor: nil}} =
               Routes.list(instance, cursor: "r01998", limit: 200)

      assert Enum.map(page, & &1.id) == ["r01999", "r02000"]

      # Every page, end to end: each route exactly once.
      ids =
        Stream.unfold(nil, fn
          :done ->
            nil

          cursor ->
            {:ok, %{routes: routes, next_cursor: next}} =
              Routes.list(instance, cursor: cursor, limit: 200)

            {Enum.map(routes, & &1.id), next || :done}
        end)
        |> Enum.concat()

      assert length(ids) == 2_000
      assert ids == Enum.sort(ids)
    end
  end
end
