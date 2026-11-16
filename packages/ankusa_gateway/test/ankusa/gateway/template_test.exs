defmodule Ankusa.Gateway.TemplateTest do
  @moduledoc """
  Templates: the variable grammar and its allowed roots, values (scalars,
  lists, missing ones, fallbacks), `render_many/2` expanding one list, the
  bindings a request produces, and `compile_url!/3` keeping the origin literal
  and percent-encoding every substituted value.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.Template
  alias Ankusa.Principal

  @label "gateway.authz.authorizers.team.rules[0].when.groups_any[0]"
  @all_roots ~w(principal claims action kind space key tenant vars)

  defp compile!(string, roots \\ @all_roots), do: Template.compile!(string, @label, roots)

  defp render!(string, bindings, roots \\ @all_roots) do
    assert {:ok, rendered} = Template.render(compile!(string, roots), bindings)
    rendered
  end

  defp principal do
    %Principal{id: "alice", scopes: ["claims:read"], tenants: ["acme"], groups: ["eng"]}
  end

  describe "literals" do
    test "a template without a variable is its text" do
      assert render!("events/eng", %{}) == "events/eng"
      assert render!("", %{}) == ""
      assert render!("a }} b", %{}) == "a }} b"
    end

    property "rendering a compiled literal is the literal" do
      check all text <- string(:alphanumeric, max_length: 40) do
        assert render!(text, %{}) == text
      end
    end
  end

  describe "variables" do
    test "render the binding's value" do
      bindings = %{
        principal: principal(),
        claims: %{"team" => "eng"},
        action: :read,
        kind: :object,
        space: "docs",
        key: "teams/eng/a.txt",
        tenant: "acme",
        vars: %{"team" => "eng"}
      }

      assert render!("{{principal.id}}", bindings) == "alice"
      assert render!("{{ principal.id }}", bindings) == "alice"
      assert render!("{{ claims.team }}", bindings) == "eng"
      assert render!("{{ action }}", bindings) == "read"
      assert render!("{{ kind }}", bindings) == "object"
      assert render!("{{ space }}", bindings) == "docs"
      assert render!("{{ key }}", bindings) == "teams/eng/a.txt"
      assert render!("{{ tenant }}", bindings) == "acme"
      assert render!("{{ vars.team }}", bindings) == "eng"
      assert render!("{{ vars.team }}-{{ claims.team }}", bindings) == "eng-eng"
    end

    test "walk nested claim maps" do
      bindings = %{claims: %{"org" => %{"team" => "eng"}}}

      assert render!("{{ claims.org.team }}", bindings) == "eng"

      assert Template.render(compile!("{{ claims.org.missing }}"), bindings) ==
               {:error, {:template_missing, "claims.org.missing"}}
    end

    test "render integers and booleans" do
      assert render!("{{ claims.count }}", %{claims: %{"count" => 42}}) == "42"
      assert render!("{{ claims.admin }}", %{claims: %{"admin" => true}}) == "true"
      assert render!("{{ claims.admin }}", %{claims: %{"admin" => false}}) == "false"
    end

    test "an absent binding, a nil, an empty value and a control character are missing" do
      assert Template.render(compile!("{{ tenant }}"), %{}) ==
               {:error, {:template_missing, "tenant"}}

      assert Template.render(compile!("{{ tenant }}"), %{tenant: nil}) ==
               {:error, {:template_missing, "tenant"}}

      assert Template.render(compile!("{{ space }}"), %{space: ""}) ==
               {:error, {:template_missing, "space"}}

      assert Template.render(compile!("{{ key }}"), %{key: "a\nb"}) ==
               {:error, {:template_missing, "key"}}

      assert Template.render(compile!("{{ key }}"), %{key: <<0x7F>>}) ==
               {:error, {:template_missing, "key"}}
    end
  end

  describe "fallbacks" do
    test "stand in for a missing value and only for one" do
      template = compile!("tenant/{{ claims.tid ?? default }}")

      assert Template.render(template, %{claims: %{}}) == {:ok, "tenant/default"}
      assert Template.render(template, %{claims: %{"tid" => "acme"}}) == {:ok, "tenant/acme"}
    end

    test "a fallback with other characters is refused" do
      assert_raise ArgumentError, ~r/a fallback must be literal text/, fn ->
        compile!("{{ claims.tid ?? a b }}")
      end
    end

    test "more than one ?? is refused" do
      assert_raise ArgumentError, ~r/more than one \?\?/, fn ->
        compile!("{{ claims.tid ?? a ?? b }}")
      end
    end
  end

  describe "lists" do
    test "render/2 refuses a list-valued variable and names it" do
      assert Template.render(compile!("{{ principal.groups }}"), %{principal: principal()}) ==
               {:error, {:template_list, "principal.groups"}}

      assert Template.render(compile!("{{ principal.scopes }}"), %{principal: principal()}) ==
               {:error, {:template_list, "principal.scopes"}}
    end

    test "render_many/2 expands the one list into one string per element" do
      bindings = %{principal: %Principal{id: "alice", groups: ["eng", "ops"]}}

      assert Template.render_many(compile!("team-{{ principal.groups }}"), bindings) ==
               {:ok, ["team-eng", "team-ops"]}
    end

    test "render_many/2 keeps the text around each value" do
      bindings = %{principal: %Principal{id: "alice", groups: ["eng", "ops"]}}

      assert Template.render_many(compile!("g/{{ principal.groups }}/x"), bindings) ==
               {:ok, ["g/eng/x", "g/ops/x"]}
    end

    test "render_many/2 with no list renders one string" do
      assert Template.render_many(compile!("{{ principal.id }}"), %{principal: principal()}) ==
               {:ok, ["alice"]}
    end

    test "render_many/2 refuses two lists" do
      template = compile!("{{ principal.groups }}:{{ principal.scopes }}")

      assert Template.render_many(template, %{principal: principal()}) ==
               {:error, {:template_lists, ["principal.groups", "principal.scopes"]}}
    end

    test "principal.tenants of :all renders as [\"*\"]" do
      bindings = %{principal: %Principal{id: "alice", tenants: :all}}
      template = compile!("{{ principal.tenants }}")

      assert Template.render(template, bindings) ==
               {:error, {:template_list, "principal.tenants"}}

      assert Template.render_many(template, bindings) == {:ok, ["*"]}
    end

    test "an empty list is a missing value" do
      bindings = %{principal: %Principal{id: "alice", groups: []}}

      assert Template.render(compile!("{{ principal.groups }}"), bindings) ==
               {:error, {:template_missing, "principal.groups"}}
    end

    test "render_many/2 reports a missing value" do
      assert Template.render_many(compile!("{{ claims.team }}"), %{claims: %{}}) ==
               {:error, {:template_missing, "claims.team"}}
    end
  end

  describe "compile!/3" do
    test "refuses a root the caller did not allow" do
      assert_raise ArgumentError, ~r/is not allowed here \(allowed roots: vars, space\)/, fn ->
        Template.compile!("{{ claims.team }}", @label, ["vars", "space"])
      end
    end

    test "allows exactly the roots it is given" do
      assert Template.render(Template.compile!("{{ vars.a }}", @label, ["vars"]), %{
               vars: %{"a" => "x"}
             }) == {:ok, "x"}
    end

    test "refuses an unknown root" do
      assert_raise ArgumentError, ~r/unknown template variable root "bogus"/, fn ->
        compile!("{{ bogus }}")
      end

      assert_raise ArgumentError, ~r/unknown template variable root "bogus"/, fn ->
        Template.compile!("unused", @label, ["bogus"])
      end
    end

    test "refuses a variable of the wrong shape for its root" do
      for template <- [
            "{{ principal.email }}",
            "{{ principal }}",
            "{{ principal.id.extra }}",
            "{{ action.x }}",
            "{{ claims }}",
            "{{ vars }}",
            "{{ vars.a.b }}"
          ] do
        assert_raise ArgumentError, ~r/unknown template variable/, fn -> compile!(template) end
      end
    end

    test "refuses an unclosed {{ and an empty {{ }}" do
      assert_raise ArgumentError, ~r/^#{Regex.escape(@label)}: unclosed \{\{/, fn ->
        compile!("a {{ claims.tid")
      end

      assert_raise ArgumentError, ~r/empty \{\{ \}\}/, fn -> compile!("a {{  }} b") end
    end

    test "refuses a template that is not a string" do
      assert_raise ArgumentError, ~r/a template must be a string/, fn ->
        Template.compile!(:nope, @label, @all_roots)
      end
    end
  end

  describe "compile_url!/3" do
    test "refuses a template in the origin" do
      assert_raise ArgumentError,
                   "#{@label}: the scheme, host and port of a URL cannot be templated",
                   fn ->
                     Template.compile_url!("https://{{ claims.t }}.example/x", @label, @all_roots)
                   end
    end

    test "refuses a URL with no literal origin" do
      assert_raise ArgumentError,
                   ~r/the scheme, host and port of a URL cannot be templated/,
                   fn ->
                     Template.compile_url!("{{ vars.u }}/x", @label, @all_roots)
                   end
    end

    test "percent-encodes every substituted value, and only those" do
      template =
        Template.compile_url!("https://idp.example/u/{{ vars.u }}/x?q={{ vars.q }}", @label, [
          "vars"
        ])

      assert Template.render(template, %{vars: %{"u" => "a/b?c=d", "q" => "a b"}}) ==
               {:ok, "https://idp.example/u/a%2Fb%3Fc%3Dd/x?q=a%20b"}
    end

    test "leaves unreserved characters alone" do
      template = Template.compile_url!("https://idp.example/{{ vars.u }}", @label, ["vars"])

      assert Template.render(template, %{vars: %{"u" => "a-b_c.d~e"}}) ==
               {:ok, "https://idp.example/a-b_c.d~e"}
    end

    test "encodes each element of a list and the fallback" do
      template =
        Template.compile_url!("https://idp.example/{{ principal.groups }}", @label, [
          "principal"
        ])

      assert Template.render_many(template, %{principal: %Principal{id: "a", groups: ["a/b"]}}) ==
               {:ok, ["https://idp.example/a%2Fb"]}

      fallback =
        Template.compile_url!("https://idp.example/{{ vars.u ?? a/b }}", @label, ["vars"])

      assert Template.render(fallback, %{vars: %{}}) == {:ok, "https://idp.example/a%2Fb"}
    end
  end

  describe "bindings/2" do
    test "builds the bindings of a request" do
      request = %Request{
        principal: principal(),
        via: :token,
        subject_key: <<0::128>>,
        action: :write,
        resource: %{kind: :object, space: "docs", key: "teams/eng/a.txt", tenant: nil}
      }

      bindings = Template.bindings(request, %{"team" => "eng"})
      assert bindings.principal.id == "alice"
      assert bindings.claims == %{}
      assert bindings.action == :write
      assert bindings.kind == :object
      assert bindings.space == "docs"
      assert bindings.key == "teams/eng/a.txt"
      assert bindings.tenant == nil
      assert bindings.vars == %{"team" => "eng"}

      assert Template.render(compile!("{{ key }}:{{ vars.team }}"), bindings) ==
               {:ok, "teams/eng/a.txt:eng"}
    end

    test "a claim's resource carries its tenant" do
      request = %Request{
        principal: principal(),
        via: :token,
        subject_key: <<0::128>>,
        action: :read,
        resource: %{
          kind: :claim,
          space: "claims",
          key: "acme/01M39VMD8RA3C5HR4RBV67Y002",
          tenant: "acme"
        }
      }

      bindings = Template.bindings(request)

      assert Template.render(compile!("{{ tenant }}"), bindings) == {:ok, "acme"}

      assert Template.render(compile!("{{ vars.team }}"), bindings) ==
               {:error, {:template_missing, "vars.team"}}
    end
  end

  describe "claim/2 and scalar/1" do
    test "claim/2 walks a dotted path or a segment list" do
      claims = %{"team" => "eng", "org" => %{"name" => "acme"}, "count" => 3}

      assert Template.claim(claims, "team") == {:ok, "eng"}
      assert Template.claim(claims, "org.name") == {:ok, "acme"}
      assert Template.claim(claims, ["org", "name"]) == {:ok, "acme"}
      assert Template.claim(claims, "org.missing") == :error
      assert Template.claim(claims, "missing") == :error
      assert Template.claim(claims, "count.value") == :error
    end

    test "scalar/1 formats what a template substitutes" do
      assert Template.scalar("eng") == {:ok, "eng"}
      assert Template.scalar(3) == {:ok, "3"}
      assert Template.scalar(true) == {:ok, "true"}
      assert Template.scalar(:read) == {:ok, "read"}
      assert Template.scalar("") == :error
      assert Template.scalar(nil) == :error
      assert Template.scalar(["eng"]) == :error
    end
  end
end
