# ankusa_oidc

OpenID Connect bearer-token verifier for the Ankusa admin API.

`ankusa` core can require a bearer token on its admin and route-management
listeners (`auth.enabled`), but verifying a JWT needs `jose`, so the verifier
lives here: only deployments that turn single sign-on on pull it in.
`Ankusa.Auth.OIDC` checks that a token was signed by your identity provider
(Okta, Entra ID, Keycloak, Auth0, Google...) for your audience, with the keys it
fetches from the provider's JWKS endpoint and caches.

It decides *who a token is*. What that person may do (roles, tenants) is
`auth.bindings`; who they are in your directory is SCIM (`scim.enabled`). See
the `Ankusa.Auth` moduledoc.

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.5"},
    {:ankusa_oidc, "~> 0.1"}
  ]
end
```

## Usage

```elixir
config = Ankusa.Config.new(
  admin: %{enabled: true},
  auth: %{
    enabled: true,
    verifier:
      {Ankusa.Auth.OIDC,
       issuer: "https://login.example.com/",
       audience: "ankusa"},
    bindings: [%{group: "Platform", role: :admin}]
  }
)
```

The `jamescarr/ankusa` image has this package built in; there it is the
`auth.oidc` block of the YAML config.

See `Ankusa.Auth.OIDC` for every option and exactly what is checked.

## Testing

```sh
mise run check:package ankusa_oidc
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
Ankusa documentation.
