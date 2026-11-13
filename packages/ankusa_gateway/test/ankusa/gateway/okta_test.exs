defmodule Ankusa.Gateway.OktaTest do
  @moduledoc """
  `type: :okta`: Okta's issuer layout, expanded at boot into a `jwt` or an
  `introspection` authenticator (`Ankusa.Gateway.Config.new/2`).
  """

  use ExUnit.Case, async: true

  alias Ankusa.Gateway.Config

  # The chain entry `given` builds, expanded.
  defp okta(given) do
    config =
      Config.new(
        auth: %{authenticators: [Map.merge(%{type: :okta, domain: "acme.okta.com"}, given)]}
      )

    hd(config.auth.authenticators)
  end

  test "a custom authorization server is a jwt authenticator for its issuer" do
    authenticator = okta(%{audience: "api://default"})

    assert %{type: :jwt, module: Ankusa.Gateway.Authn.JWT, name: "okta"} = authenticator
    assert authenticator.config.issuer == "https://acme.okta.com/oauth2/default"
    assert authenticator.config.jwks_url == "https://acme.okta.com/oauth2/default/v1/keys"
    assert authenticator.config.groups_claim == "groups"
    assert authenticator.config.audience == "api://default"
  end

  test "the authorization server names the issuer path; the name may be given" do
    authenticator =
      okta(%{authorization_server: "corp", audience: "api://corp", name: "okta-corp"})

    assert authenticator.name == "okta-corp"
    assert authenticator.config.issuer == "https://acme.okta.com/oauth2/corp"
    assert authenticator.config.jwks_url == "https://acme.okta.com/oauth2/corp/v1/keys"
  end

  test "a jwt option in the entry overrides what the expansion derives" do
    authenticator =
      okta(%{audience: "api://default", subject_prefix: "okta|", groups_claim: "roles"})

    assert authenticator.config.subject_prefix == "okta|"
    assert authenticator.config.groups_claim == "roles"
    assert authenticator.config.issuer == "https://acme.okta.com/oauth2/default"
  end

  test "a key the expanded type does not know is refused by it" do
    assert_raise ArgumentError,
                 "unknown Ankusa.Gateway.Config key: gateway.auth.authenticators[0].url",
                 fn ->
                   okta(%{
                     audience: "api://default",
                     url: "https://acme.okta.com/oauth2/v1/introspect"
                   })
                 end
  end

  test "the org authorization server is an introspection authenticator with the app's credentials" do
    authenticator =
      okta(%{
        authorization_server: "org",
        client_id: "0oa1",
        client_secret: "s3cret",
        timeout_ms: 1500
      })

    assert %{type: :introspection, module: Ankusa.Gateway.Authn.Introspection, name: "okta"} =
             authenticator

    assert authenticator.config.url == "https://acme.okta.com/oauth2/v1/introspect"
    assert authenticator.config.issuer == "https://acme.okta.com"
    assert authenticator.config.client_id == "0oa1"
    assert authenticator.config.client_secret == "s3cret"
    assert authenticator.config.timeout_ms == 1500
  end

  test "the org form needs both credentials and the jwt form needs an audience" do
    assert_raise ArgumentError,
                 ~r/gateway\.auth\.authenticators\[0\]\.client_id must be set/,
                 fn -> okta(%{authorization_server: "org", client_secret: "s"}) end

    assert_raise ArgumentError,
                 ~r/gateway\.auth\.authenticators\[0\]\.client_secret must be set/,
                 fn -> okta(%{authorization_server: "org", client_id: "c"}) end

    assert_raise ArgumentError, "gateway.auth.authenticators[0].audience must be set", fn ->
      okta(%{})
    end
  end

  test "the domain and the authorization server name are checked, without a call" do
    for {given, message} <- [
          {%{domain: nil}, ~r/\.domain must be a lowercase host name/},
          {%{domain: "Acme.Okta.com"}, ~r/\.domain must be a lowercase host name/},
          {%{domain: "acme.okta.com/oauth2"}, ~r/\.domain must be a lowercase host name/},
          {%{authorization_server: "Org"}, ~r/\.authorization_server must be "org"/}
        ] do
      assert_raise ArgumentError, message, fn -> okta(Map.put(given, :audience, "api://x")) end
    end

    assert_raise ArgumentError,
                 "unknown Ankusa.Gateway.Config key: gateway.auth.authenticators[0].domian",
                 fn -> okta(%{domian: "acme.okta.com", audience: "api://x"}) end
  end

  test "two unnamed okta entries collide on the chain's one name" do
    assert_raise ArgumentError, "gateway.auth.authenticators: two entries are named okta", fn ->
      Config.new(
        auth: %{
          authenticators: [
            %{type: :okta, domain: "a.okta.com", audience: "api://a"},
            %{type: :okta, domain: "b.okta.com", audience: "api://b"}
          ]
        }
      )
    end
  end
end
