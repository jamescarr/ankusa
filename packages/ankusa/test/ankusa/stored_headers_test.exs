defmodule Ankusa.StoredHeadersTest do
  use ExUnit.Case, async: true

  alias Ankusa.{Config, Source}
  alias Ankusa.Edge.StoredHeaders

  test "the default drops the credential-bearing headers and hashes nothing" do
    assert StoredHeaders.default() == %{
             drop: ~w(authorization proxy-authorization cookie set-cookie x-api-key x-auth-token),
             hash: []
           }

    assert %Config{store_headers: default} = Config.new()
    assert default == StoredHeaders.default()
  end

  test "strip removes dropped names (any case) and counts them" do
    headers = [
      {"content-type", "application/json"},
      {"Authorization", "Bearer s3cret"},
      {"cookie", "sid=abc"},
      {"x-github-event", "push"}
    ]

    assert {[{"content-type", "application/json"}, {"x-github-event", "push"}], 2} =
             StoredHeaders.strip(headers, StoredHeaders.default())
  end

  test "strip replaces a hashed value with sha256:<hex> and keeps the name and order" do
    policy = %{drop: [], hash: ["x-tenant-token"]}
    headers = [{"a", "1"}, {"X-Tenant-Token", "hunter2"}, {"b", "2"}]

    assert {[{"a", "1"}, {"X-Tenant-Token", digest}, {"b", "2"}], 0} =
             StoredHeaders.strip(headers, policy)

    assert digest == "sha256:" <> Base.encode16(:crypto.hash(:sha256, "hunter2"), case: :lower)
  end

  test "a name in both lists is dropped, not hashed" do
    assert {[], 1} = StoredHeaders.strip([{"cookie", "x"}], %{drop: ["cookie"], hash: ["cookie"]})
  end

  test "a list the policy does not touch comes back as the same list" do
    headers = [{"content-type", "text/plain"}, {"x-event", "1"}]
    assert {same, 0} = StoredHeaders.strip(headers, StoredHeaders.default())
    assert :erts_debug.same(same, headers)
    assert {[], 0} = StoredHeaders.strip([], StoredHeaders.default())
  end

  test "normalize! lowercases, fills an absent key from the default and names the key on error" do
    assert %{drop: ["x-secret"], hash: ["x-trace"]} =
             StoredHeaders.normalize!(%{drop: ["X-Secret"], hash: ["X-Trace"]}, "store_headers")

    assert %{drop: ["a"], hash: []} = StoredHeaders.normalize!([drop: ["A"]], "store_headers")
    assert StoredHeaders.normalize!(%{}, "store_headers") == StoredHeaders.default()

    assert_raise ArgumentError, ~r/store_headers must be a map with :drop and :hash lists/, fn ->
      StoredHeaders.normalize!("authorization", "store_headers")
    end

    assert_raise ArgumentError,
                 ~s(store_headers.drop must be a list of header names, got "authorization"),
                 fn -> StoredHeaders.normalize!(%{drop: "authorization"}, "store_headers") end

    assert_raise ArgumentError, ~r/store_headers.hash must be a list of header names/, fn ->
      StoredHeaders.normalize!(%{hash: [""]}, "store_headers")
    end

    assert_raise ArgumentError, "unknown Ankusa.Config key: store_headers.keep", fn ->
      StoredHeaders.normalize!(%{keep: []}, "store_headers")
    end
  end

  test "Config.new validates the policy at boot" do
    assert Config.new(store_headers: %{drop: ["X-Foo"]}).store_headers ==
             %{drop: ["x-foo"], hash: []}

    assert_raise ArgumentError, ~r/store_headers.drop must be a list of header names/, fn ->
      Config.new(store_headers: %{drop: :authorization})
    end
  end

  test "a source's override replaces the instance's list key by key" do
    source = Source.new("s", store_headers: %{hash: ["X-Token"]})
    assert source.store_headers == %{drop: nil, hash: ["x-token"]}
    assert Source.new("s", []).store_headers == nil

    instance = %{drop: ["cookie"], hash: []}

    assert StoredHeaders.resolve(source.store_headers, instance) == %{
             drop: ["cookie"],
             hash: ["x-token"]
           }

    assert StoredHeaders.resolve(nil, instance) == instance

    assert_raise ArgumentError, ~r/source "s": store_headers.drop must be a list/, fn ->
      Source.new("s", store_headers: %{drop: "cookie"})
    end
  end
end
