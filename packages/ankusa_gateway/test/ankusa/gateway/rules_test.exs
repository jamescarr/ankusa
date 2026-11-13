defmodule Ankusa.Gateway.Authz.RulesTest do
  @moduledoc """
  The `rules` authorizer: first match wins, the key pattern's captures feeding
  the `when` templates, every predicate, the claims API and prefix requests,
  and the boot-time validation of every field.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Gateway.Authz.{Request, Rules}
  alias Ankusa.Principal

  @key "gateway.authz.authorizers.team"

  defp principal(fields \\ []) do
    %Principal{
      id: Keyword.get(fields, :id, "alice"),
      scopes: Keyword.get(fields, :scopes, ["claims:read"]),
      tenants: Keyword.get(fields, :tenants, ["acme"]),
      groups: Keyword.get(fields, :groups, ["eng"]),
      claims: Keyword.get(fields, :claims, %{})
    }
  end

  defp request(fields \\ []) do
    resource = %{
      kind: Keyword.get(fields, :kind, :object),
      space: Keyword.get(fields, :space, "docs"),
      key: Keyword.get(fields, :key, "teams/eng/a.txt"),
      tenant: Keyword.get(fields, :tenant)
    }

    %Request{
      principal: Keyword.get(fields, :principal, principal()),
      via: :token,
      subject_key: <<0::128>>,
      action: Keyword.get(fields, :action, :read),
      resource: resource
    }
  end

  defp state(rules), do: Rules.init!(%{rules: rules}, @key)

  defp decide(rules, request) do
    Rules.decide(request, state(rules), %{instance: :default, name: "team", deadline: 0})
  end

  defp allow, do: {:ok, :allow, %{}}
  defp deny, do: {:ok, :deny, %{}}
  defp abstain, do: {:ok, :abstain, %{}}

  describe "order and effects" do
    test "the first matching rule's effect decides" do
      rules = [
        %{effect: :allow, actions: ["read"], spaces: ["docs"]},
        %{effect: :deny, actions: ["*"]}
      ]

      assert decide(rules, request()) == allow()
    end

    test "a deny before an allow wins" do
      rules = [
        %{effect: :deny, actions: ["read"]},
        %{effect: :allow, actions: ["*"]}
      ]

      assert decide(rules, request()) == deny()
    end

    test "an effect may be the string allow or deny" do
      assert decide([%{effect: "allow", actions: ["*"]}], request()) == allow()
      assert decide([%{effect: "deny", actions: ["*"]}], request()) == deny()
    end

    test "no rule matching is an abstain" do
      assert decide([%{effect: :allow, actions: ["write"]}], request()) == abstain()

      assert decide([%{effect: :allow, actions: ["read"], spaces: ["other"]}], request()) ==
               abstain()

      assert decide([%{effect: :allow, actions: ["read"], keys: ["reports/**"]}], request()) ==
               abstain()
    end

    test "a rule with no when predicate matches every key in its action and space" do
      rules = [%{effect: :allow, actions: ["read"]}]

      assert decide(rules, request(key: "anything/at/all")) == allow()
    end

    test "* as an action and as a space means every one" do
      rules = [%{effect: :allow, actions: ["*"], spaces: ["*"]}]

      assert decide(rules, request(action: :share, space: "reports")) == allow()
    end

    test "sixty-four rules are evaluated in order" do
      misses =
        for index <- 1..63 do
          %{effect: :deny, actions: ["read"], spaces: ["space-#{index}"]}
        end

      assert decide(misses ++ [%{effect: :allow, actions: ["read"]}], request()) == allow()

      assert decide(misses ++ [%{effect: :deny, actions: ["read"]}], request()) == deny()
    end

    test "kind/1 is inline" do
      assert Rules.kind(%{}) == :inline
    end
  end

  describe "key patterns" do
    test "a capture becomes vars for the rule's templates" do
      rules = [
        %{
          effect: :allow,
          actions: ["read"],
          spaces: ["docs"],
          keys: ["teams/{team}/**"],
          when: %{groups_any: ["team-{{ vars.team }}"]}
        }
      ]

      assert decide(rules, request(principal: principal(groups: ["team-eng"]))) == allow()
      assert decide(rules, request(principal: principal(groups: ["team-ops"]))) == abstain()
    end

    test "a capture feeds a claims predicate" do
      rules = [
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["releases/{channel}/**"],
          when: %{claims: %{"branch" => ["{{ vars.channel }}"]}}
        }
      ]

      main = principal(claims: %{"branch" => "main"})
      assert decide(rules, request(principal: main, key: "releases/main/x")) == allow()
      assert decide(rules, request(principal: main, key: "releases/dev/x")) == abstain()
    end

    test "a prefix request is matched with its trailing slash removed" do
      rules = [%{effect: :allow, actions: ["list"], keys: ["reports/**"]}]

      assert decide(rules, request(action: :list, kind: :prefix, key: "reports/")) == allow()
      assert decide(rules, request(action: :list, kind: :prefix, key: "")) == abstain()
    end

    test "a claims API key is <tenant>/<claim_id>" do
      rules = [
        %{
          effect: :allow,
          actions: ["read"],
          spaces: ["claims"],
          keys: ["{tenant}/**"],
          when: %{tenants_any: ["{{ vars.tenant }}"]}
        }
      ]

      claim =
        request(
          kind: :claim,
          space: "claims",
          key: "acme/01M39VMD8RA3C5HR4RBV67Y002",
          tenant: "acme"
        )

      assert decide(rules, claim) == allow()

      other = %{claim | principal: principal(tenants: ["other"])}
      assert decide(rules, other) == abstain()

      document = %{claim | resource: %{claim.resource | space: "docs"}}
      assert decide(rules, document) == abstain()
    end
  end

  describe "when predicates" do
    test "groups_any and groups_all" do
      any = [%{effect: :allow, actions: ["read"], when: %{groups_any: ["eng", "ops"]}}]
      all = [%{effect: :allow, actions: ["read"], when: %{groups_all: ["eng", "ops"]}}]

      assert decide(any, request(principal: principal(groups: ["ops"]))) == allow()
      assert decide(any, request(principal: principal(groups: ["sales"]))) == abstain()
      assert decide(all, request(principal: principal(groups: ["ops", "eng"]))) == allow()
      assert decide(all, request(principal: principal(groups: ["eng"]))) == abstain()
    end

    test "scopes read the principal's scopes and the token's scope claims" do
      rules = [%{effect: :allow, actions: ["read"], when: %{scopes_any: ["docs:read"]}}]

      assert decide(rules, request()) == abstain()

      assert decide(rules, request(principal: principal(scopes: ["docs:read"]))) == allow()

      scope_claim = principal(claims: %{"scope" => "openid docs:read"})
      assert decide(rules, request(principal: scope_claim)) == allow()

      scp_claim = principal(claims: %{"scp" => ["docs:read", "profile"]})
      assert decide(rules, request(principal: scp_claim)) == allow()

      all = [
        %{effect: :allow, actions: ["read"], when: %{scopes_all: ["claims:read", "docs:read"]}}
      ]

      assert decide(all, request(principal: scope_claim)) == allow()
      assert decide(all, request(principal: principal(scopes: ["claims:read"]))) == abstain()
    end

    test "tenants_any, where :all matches every template" do
      rules = [%{effect: :allow, actions: ["read"], when: %{tenants_any: ["acme", "globex"]}}]

      assert decide(rules, request()) == allow()
      assert decide(rules, request(principal: principal(tenants: ["initech"]))) == abstain()
      assert decide(rules, request(principal: principal(tenants: :all))) == allow()
    end

    test "principals are exact ids" do
      rules = [%{effect: :allow, actions: ["read"], when: %{principals: ["alice"]}}]

      assert decide(rules, request()) == allow()
      assert decide(rules, request(principal: principal(id: "bob"))) == abstain()
    end

    test "claims compare with a string, a nested path, a list and a number" do
      rules = [
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["a/**"],
          when: %{claims: %{"team" => ["eng"]}}
        },
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["b/**"],
          when: %{claims: %{"org.name" => ["acme"]}}
        },
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["c/**"],
          when: %{claims: %{"roles" => ["admin"]}}
        },
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["d/**"],
          when: %{claims: %{"org_id" => ["42"]}}
        }
      ]

      claims = %{
        "team" => "eng",
        "org" => %{"name" => "acme"},
        "roles" => ["reader", "admin"],
        "org_id" => 42
      }

      principal = principal(claims: claims)

      for key <- ["a/x", "b/x", "c/x", "d/x"] do
        assert decide(rules, request(principal: principal, key: key)) == allow()
        assert decide(rules, request(key: key)) == abstain()
      end
    end

    test "a template that cannot render makes its predicate false" do
      rules = [
        %{
          effect: :allow,
          actions: ["read"],
          keys: ["**"],
          when: %{groups_any: ["{{ vars.team }}"]}
        }
      ]

      assert decide(rules, request(principal: principal(groups: ["eng"]))) == abstain()
    end
  end

  describe "validation" do
    test "rules are required and bounded" do
      assert_raise ArgumentError, ~r/\.rules must be a list of 1 to 256 rules, got \[\]/, fn ->
        state([])
      end

      assert_raise ArgumentError, ~r/\.rules must be a list of 1 to 256 rules, got nil/, fn ->
        Rules.init!(%{}, @key)
      end

      too_many = for _index <- 1..257, do: %{effect: :allow, actions: ["*"]}

      assert_raise ArgumentError, ~r/list of 1 to 256 rules/, fn -> state(too_many) end
    end

    test "an unknown option is refused beside the dotted key" do
      assert_raise ArgumentError,
                   "unknown Ankusa.Gateway.Config key: #{@key}.extra",
                   fn ->
                     Rules.init!(%{rules: [%{effect: :allow, actions: ["*"]}], extra: 1}, @key)
                   end

      assert_raise ArgumentError,
                   ~r/unknown Ankusa.Gateway.Config key: .+\.rules\[0\]\.foo/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], foo: 1}])
                   end

      assert_raise ArgumentError,
                   ~r/unknown Ankusa.Gateway.Config key: .+\.rules\[0\]\.when\.nope/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{nope: ["x"]}}])
                   end
    end

    test "a rule must be a map" do
      assert_raise ArgumentError, ~r/\.rules\[0\] must be a map or keyword list/, fn ->
        state([:nope])
      end
    end

    test "effect is required and is allow or deny" do
      assert_raise ArgumentError, ~r/\.rules\[0\]\.effect must be allow or deny, got nil/, fn ->
        state([%{actions: ["read"]}])
      end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.effect must be allow or deny, got :maybe/,
                   fn ->
                     state([%{effect: :maybe, actions: ["read"]}])
                   end
    end

    test "actions are required, known and unmixed" do
      assert_raise ArgumentError, ~r/\.rules\[0\]\.actions must be a non-empty list/, fn ->
        state([%{effect: :allow}])
      end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.actions must be a non-empty list/, fn ->
        state([%{effect: :allow, actions: []}])
      end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.actions has unknown action "publish"/, fn ->
        state([%{effect: :allow, actions: ["publish"]}])
      end

      assert_raise ArgumentError, ~r/"\*" cannot be combined with other actions/, fn ->
        state([%{effect: :allow, actions: ["read", "*"]}])
      end

      assert %{rules: [%{actions: [:read, :write]}]} =
               state([%{effect: :allow, actions: [:read, "write"]}])

      assert %{rules: [%{actions: :all}]} = state([%{effect: :allow, actions: [:*]}])
    end

    test "spaces are names or *" do
      assert_raise ArgumentError, ~r/\.rules\[0\]\.spaces has invalid space name "Docs"/, fn ->
        state([%{effect: :allow, actions: ["*"], spaces: ["Docs"]}])
      end

      assert_raise ArgumentError, ~r/"\*" cannot be combined with other spaces/, fn ->
        state([%{effect: :allow, actions: ["*"], spaces: ["*", "docs"]}])
      end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.spaces must be a non-empty list/, fn ->
        state([%{effect: :allow, actions: ["*"], spaces: []}])
      end

      assert %{rules: [%{spaces: :all}]} =
               state([%{effect: :allow, actions: ["*"], spaces: ["*"]}])

      assert %{rules: [%{spaces: ["claims"]}]} =
               state([%{effect: :allow, actions: ["*"], spaces: [:claims]}])
    end

    test "keys are compiled patterns, named by their index" do
      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.keys\[1\]: \*\* must be the last segment/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], keys: ["a/**", "**/b"]}])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.keys must be a non-empty list of patterns/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], keys: []}])
                   end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.keys\[0\]: a pattern must be a string/, fn ->
        state([%{effect: :allow, actions: ["*"], keys: [42]}])
      end
    end

    test "when is a map of known predicates, each a non-empty list" do
      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.groups_any must be a non-empty list/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{groups_any: []}}])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.groups_all must be a non-empty list/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{groups_all: :eng}}])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.principals must be a non-empty list/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{principals: []}}])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.principals must be a list of strings/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{principals: ["alice", 1]}}])
                   end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.when must be a map or keyword list/, fn ->
        state([%{effect: :allow, actions: ["*"], when: ["eng"]}])
      end
    end

    test "when templates may not use principal or claims" do
      assert_raise ArgumentError,
                   ~r/is not allowed here \(allowed roots: vars, space, tenant, action\)/,
                   fn ->
                     state([
                       %{
                         effect: :allow,
                         actions: ["*"],
                         when: %{groups_any: ["{{ principal.id }}"]}
                       }
                     ])
                   end

      assert_raise ArgumentError, ~r/is not allowed here/, fn ->
        state([%{effect: :allow, actions: ["*"], when: %{scopes_any: ["{{ claims.scope }}"]}}])
      end

      assert_raise ArgumentError, ~r/\.rules\[0\]\.when\.groups_any\[0\]/, fn ->
        state([%{effect: :allow, actions: ["*"], when: %{groups_any: ["{{ "]}}])
      end
    end

    test "when.claims names dotted claim paths" do
      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.claims has invalid claim path "a..b"/,
                   fn ->
                     state([
                       %{effect: :allow, actions: ["*"], when: %{claims: %{"a..b" => ["x"]}}}
                     ])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.claims must be a map of dotted claim paths/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{claims: %{}}}])
                   end

      assert_raise ArgumentError,
                   ~r/\.rules\[0\]\.when\.claims\.team must be a non-empty list/,
                   fn ->
                     state([%{effect: :allow, actions: ["*"], when: %{claims: %{"team" => []}}}])
                   end

      assert %{rules: [%{predicates: %{claims: [{"org.team", [_template]}]}}]} =
               state([
                 %{effect: :allow, actions: ["*"], when: %{claims: %{"org.team" => ["eng"]}}}
               ])
    end
  end
end
