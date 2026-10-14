defmodule Ankusa.Scim.FilterTest do
  use ExUnit.Case, async: true

  alias Ankusa.Scim.Filter

  describe "the lookups an identity provider makes before creating a resource" do
    test "userName eq, as Okta probes it" do
      assert Filter.parse(:user, ~s(userName eq "Jane@Corp.com")) ==
               {:ok, {:user_name, "Jane@Corp.com"}}
    end

    test "externalId and id" do
      assert Filter.parse(:user, ~s(externalId eq "00u1")) == {:ok, {:external_id, "00u1"}}
      assert Filter.parse(:user, ~s(id eq "abc")) == {:ok, {:id, "abc"}}
    end

    test "displayName eq on groups" do
      assert Filter.parse(:group, ~s(displayName eq "Ops")) == {:ok, {:display_name, "Ops"}}
    end

    test "the attribute and operator are case-insensitive, the value is not" do
      assert Filter.parse(:user, ~s(USERNAME EQ "Jane")) == {:ok, {:user_name, "Jane"}}
      assert Filter.parse(:user, ~s(  userName   Eq   "Jane"  )) == {:ok, {:user_name, "Jane"}}
    end

    test "inside the quotes only \\\" and \\\\ are escapes" do
      assert Filter.parse(:user, ~S(userName eq "a\"b\\c")) == {:ok, {:user_name, ~S(a"b\c)}}
      assert {:error, _} = Filter.parse(:user, ~S(userName eq "a\nb"))
    end

    test "a value may hold anything else, including the words of the grammar" do
      assert Filter.parse(:user, ~s(userName eq "x eq \\"y\\" and z")) ==
               {:ok, {:user_name, ~s(x eq "y" and z)}}
    end
  end

  describe "what is refused" do
    test "other operators" do
      for op <- ~w(co sw ew ne gt lt pr) do
        assert {:error, detail} = Filter.parse(:user, ~s(userName #{op} "x"))
        assert detail =~ "not supported"
      end
    end

    test "compound filters and an unquoted value" do
      assert {:error, _} = Filter.parse(:user, ~s(userName eq "a" and active eq "true"))
      assert {:error, _} = Filter.parse(:user, ~s(userName eq a))
      assert {:error, _} = Filter.parse(:user, ~s(userName eq "a" or userName eq "b"))
      assert {:error, _} = Filter.parse(:user, "")
    end

    test "attributes that cannot be looked up" do
      assert {:error, detail} = Filter.parse(:user, ~s(name eq "x"))
      assert detail =~ "cannot filter on"
      assert {:error, _} = Filter.parse(:user, ~s(emails.value eq "x"))
      assert {:error, _} = Filter.parse(:user, ~s(displayName eq "x"))
      # a user attribute on a group, and the other way round
      assert {:error, _} = Filter.parse(:group, ~s(userName eq "x"))
    end
  end
end
