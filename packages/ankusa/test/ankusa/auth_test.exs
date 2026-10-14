defmodule Ankusa.AuthTest do
  @moduledoc """
  What configuration is refused before an instance boots, and the role/scope
  arithmetic `Ankusa.Admin.Guard` authorizes with. Resolving tokens to roles
  through the directory is in `Ankusa.Admin.GuardTest`.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Auth, Config}
  alias Ankusa.Auth.Principal
  alias Ankusa.TestHelpers.TokenTable

  @token String.duplicate("s", 32)

  defp config(auth, rest \\ []) do
    base_auth = %{enabled: true, verifier: {TokenTable, tokens: %{}}}
    Config.new(Keyword.merge([auth: Map.merge(base_auth, Map.new(auth))], rest))
  end

  defp refused(config, message) do
    assert_raise ArgumentError, message, fn -> Auth.validate_config!(config) end
  end

  describe "validate_config!/1" do
    test "off by default, and nothing to check" do
      assert :ok = Auth.validate_config!(Config.new())
    end

    test "a verifier is required once auth is on" do
      refused(
        Config.new(auth: %{enabled: true}),
        "auth.verifier must be a {module, opts} pair when auth.enabled is true"
      )

      refused(
        Config.new(auth: %{enabled: true, verifier: TokenTable}),
        "auth.verifier must be a {module, opts} pair when auth.enabled is true"
      )
    end

    test "the verifier module must be there and be a verifier" do
      refused(
        config(%{verifier: {Not.A.Module, []}}),
        ~r/auth.verifier Not.A.Module is not available/
      )

      refused(config(%{verifier: {String, []}}), ~r/does not implement/)
    end

    test "the verifier validates its own options" do
      defmodule Picky do
        @behaviour Ankusa.Auth.Verifier
        def verify(_instance, _token, _opts), do: {:error, :invalid_token}
        def validate_opts!(opts), do: Keyword.fetch!(opts, :issuer) && :ok
      end

      assert_raise KeyError, fn -> Auth.validate_config!(config(%{verifier: {Picky, []}})) end
      assert :ok = Auth.validate_config!(config(%{verifier: {Picky, issuer: "x"}}))
    end

    test "a binding names exactly one of group or subject" do
      for binding <- [
            %{role: :admin},
            %{role: :admin, group: "A", subject: "s"},
            %{role: :admin, group: ""},
            %{role: :admin, subject: 7}
          ] do
        refused(
          config(%{bindings: [%{role: :viewer, group: "ok"}, binding]}),
          "auth.bindings[1] must set exactly one of group or subject"
        )
      end
    end

    test "a binding's role is one of the three" do
      refused(
        config(%{bindings: [%{group: "A", role: :root}]}),
        ~r/auth.bindings\[0\].role must be one of \[:viewer, :operator, :admin\]/
      )

      refused(config(%{bindings: [%{group: "A"}]}), ~r/auth.bindings\[0\].role/)
      refused(config(%{bindings: [%{group: "A", role: "admin"}]}), ~r/auth.bindings\[0\].role/)
    end

    test "tenants are :all or a non-empty list of valid ids" do
      for tenants <- [
            [],
            ["bad.tenant"],
            ["ok", ""],
            "acme",
            [:acme],
            [String.duplicate("a", 65)]
          ] do
        refused(
          config(%{bindings: [%{group: "A", role: :admin, tenants: tenants}]}),
          ~r/auth.bindings\[0\].tenants/
        )
      end

      assert :ok =
               Auth.validate_config!(
                 config(%{
                   bindings: [
                     %{group: "A", role: :admin, tenants: :all},
                     %{subject: "s", role: :viewer, tenants: ["acme", "glo-bex_1"]}
                   ]
                 })
               )
    end

    test "a binding may not carry keys it does not know" do
      refused(
        config(%{bindings: [%{group: "A", role: :admin, roles: [:admin]}]}),
        ~r/unknown key/
      )
    end

    test "user_match" do
      refused(
        config(%{user_match: %{claim: "sub", attribute: :email}}),
        ~r/auth.user_match.attribute/
      )

      refused(
        config(%{user_match: %{claim: :sub, attribute: :user_name}}),
        ~r/auth.user_match.claim/
      )

      refused(
        config(%{user_match: %{claim: "", attribute: :user_name}}),
        ~r/auth.user_match.claim/
      )

      assert :ok =
               Auth.validate_config!(
                 config(%{user_match: %{claim: "oid", attribute: :external_id}})
               )
    end

    test "a partial user_match keeps the default for the key it leaves out" do
      assert %{claim: "oid", attribute: :user_name} =
               config(%{user_match: %{claim: "oid"}}).auth.user_match

      assert %{claim: "preferred_username", attribute: :external_id} =
               config(%{user_match: %{attribute: :external_id}}).auth.user_match
    end

    test "the claim names are non-empty strings" do
      refused(config(%{subject_claim: nil}), ~r/auth.subject_claim/)
      refused(config(%{groups_claim: ""}), ~r/auth.groups_claim/)
    end

    test "unknown auth keys are refused when the config is built" do
      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: auth.bindngs/, fn ->
        Config.new(auth: %{bindngs: []})
      end

      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: auth.user_match.claims/, fn ->
        Config.new(auth: %{user_match: %{claims: "x"}})
      end
    end
  end

  describe "SCIM" do
    defp scim_config(scim \\ %{}, rest \\ []) do
      config(
        %{},
        Keyword.merge(
          [
            admin: %{enabled: true},
            scim: Map.merge(%{enabled: true, token: @token}, Map.new(scim))
          ],
          rest
        )
      )
    end

    test "needs auth, the admin listener and a long token" do
      assert :ok = Auth.validate_config!(scim_config())

      refused(
        Config.new(admin: %{enabled: true}, scim: %{enabled: true, token: @token}),
        "scim.enabled requires auth.enabled"
      )

      refused(
        config(%{}, scim: %{enabled: true, token: @token}),
        "scim.enabled requires admin.enabled"
      )

      refused(scim_config(%{token: nil}), ~r/scim.token must be a string of at least 32 bytes/)
      refused(scim_config(%{token: String.duplicate("s", 31)}), ~r/scim.token/)
      refused(scim_config(%{token: 12_345}), ~r/scim.token/)
    end

    test "the token is not checked while SCIM is off" do
      assert :ok = Auth.validate_config!(Config.new(scim: %{token: "short"}))
    end

    test "the directory is a {module, opts} pair" do
      refused(
        scim_config(%{directory: Ankusa.Directory.Store.Persistent}),
        ~r/scim.directory must be a \{module, opts\} pair/
      )

      refused(scim_config(%{base_url: 5}), ~r/scim.base_url/)
    end
  end

  describe "allowed?/3" do
    defp principal(grants), do: %Principal{type: :user, grants: grants}

    test "a role includes the ones below it" do
      viewer = principal([%{role: :viewer, tenants: :all}])
      operator = principal([%{role: :operator, tenants: :all}])
      admin = principal([%{role: :admin, tenants: :all}])

      assert Auth.allowed?(viewer, :viewer, :global)
      refute Auth.allowed?(viewer, :operator, :global)
      refute Auth.allowed?(viewer, :admin, :global)
      assert Auth.allowed?(operator, :viewer, :global)
      assert Auth.allowed?(operator, :operator, :global)
      refute Auth.allowed?(operator, :admin, :global)
      assert Auth.allowed?(admin, :viewer, :global)
      assert Auth.allowed?(admin, :operator, :global)
      assert Auth.allowed?(admin, :admin, :global)
    end

    test "grants over all tenants cover every scope" do
      admin = principal([%{role: :admin, tenants: :all}])
      assert Auth.allowed?(admin, :admin, :global)
      assert Auth.allowed?(admin, :admin, {:tenant, "anything"})
    end

    test "a tenant-scoped grant covers its tenants and never :global" do
      acme = principal([%{role: :admin, tenants: ["acme", "initech"]}])

      assert Auth.allowed?(acme, :admin, {:tenant, "acme"})
      assert Auth.allowed?(acme, :viewer, {:tenant, "initech"})
      refute Auth.allowed?(acme, :admin, {:tenant, "globex"})
      refute Auth.allowed?(acme, :viewer, :global)
    end

    test "the best grant that covers the scope decides, not the highest overall" do
      mixed = principal([%{role: :admin, tenants: ["acme"]}, %{role: :viewer, tenants: :all}])

      assert Auth.allowed?(mixed, :admin, {:tenant, "acme"})
      refute Auth.allowed?(mixed, :admin, {:tenant, "globex"})
      assert Auth.allowed?(mixed, :viewer, :global)
      refute Auth.allowed?(mixed, :operator, :global)
    end

    test "no grants, no access" do
      refute Auth.allowed?(principal([]), :viewer, :global)
    end

    test "anonymous holds every grant" do
      anonymous = Auth.anonymous("1.2.3.4")
      assert anonymous.type == :anonymous
      assert anonymous.ip == "1.2.3.4"
      assert Auth.allowed?(anonymous, :admin, :global)
      assert Auth.allowed?(anonymous, :admin, {:tenant, "t"})
    end
  end

  test "the actor an audit entry records has no grants and string keys" do
    principal = %Principal{
      type: :user,
      subject: "s",
      username: "u",
      user_id: "id",
      ip: "1.1.1.1",
      grants: [%{role: :admin, tenants: :all}],
      groups: ["g"]
    }

    assert Principal.to_audit(principal) == %{
             "type" => "user",
             "subject" => "s",
             "username" => "u",
             "user_id" => "id",
             "ip" => "1.1.1.1"
           }
  end
end
