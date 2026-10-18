defmodule Ankusa.SourceStoreStaticTest do
  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.{Source, SourceStore}

  defp github(secret), do: [verifier: {Ankusa.Verifier.Hmac, scheme: :github, secret: secret}]

  defp start_static(sources, overrides \\ []) do
    config =
      test_config(
        [roles: [:edge], source_store: {SourceStore.Static, sources: sources}] ++ overrides
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp secret_of(instance, id) do
    assert {:ok, %Source{verifier: {Ankusa.Verifier.Hmac, opts}}} =
             SourceStore.fetch(instance, id)

    opts[:secret]
  end

  describe "publish/2" do
    test "a running instance serves the republished spec and names what changed" do
      instance =
        start_static(%{
          "rotating" => github("old"),
          "steady" => github("same"),
          "gone" => github("bye")
        })

      assert secret_of(instance, "rotating") == "old"

      assert {:ok, ["added", "gone", "rotating"]} =
               SourceStore.Static.publish(instance, %{
                 "rotating" => github(["new", "old"]),
                 "steady" => github("same"),
                 "added" => github("fresh")
               })

      assert secret_of(instance, "rotating") == ["new", "old"]
      assert secret_of(instance, "added") == "fresh"
      assert secret_of(instance, "steady") == "same"
      assert :error = SourceStore.fetch(instance, "gone")
      assert Enum.sort(SourceStore.list(instance)) == ["added", "rotating", "steady"]
    end

    test "publishing what is already running changes nothing" do
      instance = start_static(%{"a" => github("s")})
      assert {:ok, []} = SourceStore.Static.publish(instance, %{"a" => github("s")})
    end

    test "a spec the instance would refuse to boot with is refused and nothing is applied" do
      instance = start_static(%{"a" => github("s"), "b" => github("t")})

      assert_raise ArgumentError, ~r/source b: verifier secret is missing or undecodable/, fn ->
        SourceStore.Static.publish(instance, %{
          "a" => github("changed"),
          "b" => [verifier: {Ankusa.Verifier.Hmac, scheme: :github}]
        })
      end

      assert secret_of(instance, "a") == "s"
      assert secret_of(instance, "b") == "t"
    end

    test "a shared source with no verifier is refused like at boot" do
      instance =
        start_static(%{"a" => github("s")}, route_resolver: {Ankusa.RouteResolver.TenantPath, []})

      assert_raise ArgumentError, ~r/source open is shared/, fn ->
        SourceStore.Static.publish(instance, %{"open" => [verifier: {Ankusa.Verifier.None, []}]})
      end

      assert secret_of(instance, "a") == "s"
    end

    test "an instance on another source store is refused" do
      config = test_config(roles: [:edge], source_store: {SourceStore.Persistent, sources: %{}})
      start_supervised!({Ankusa.Instance, config})

      assert_raise ArgumentError, ~r/needs the static source store/, fn ->
        SourceStore.Static.publish(config.instance, %{})
      end
    end
  end
end
