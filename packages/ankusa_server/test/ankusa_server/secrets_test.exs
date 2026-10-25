defmodule AnkusaServer.SecretsTest do
  @moduledoc """
  Secret sources (`${file:}`, `secret_file:`, `${sm:}`) and rotation without a
  redeploy (`AnkusaServer.Reload`), from the operator's side: a file
  on disk, a secret manager that answers (or does not), and a running instance
  that verifies signed POSTs before and after.

  The secret managers are a loopback HTTP stub speaking just enough of Vault KV
  v2 and AWS `GetSecretValue`.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias AnkusaServer.{Config, ConfigError, Reload}

  @moduletag capture_log: true

  defmodule Stub do
    @moduledoc false
    # State lives in an Agent: `vault` (path => fields), `aws` (id => string or
    # {:binary, bytes}), `down?` (every request answers 503), `hits` (newest first).
    @behaviour Plug

    @impl true
    def init(agent), do: agent

    @impl true
    def call(conn, agent) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      Agent.update(
        agent,
        &%{&1 | hits: [{conn.method, conn.request_path, conn.req_headers} | &1.hits]}
      )

      state = Agent.get(agent, & &1)

      cond do
        state.down? -> Plug.Conn.send_resp(conn, 503, "down")
        conn.method == "GET" -> vault(conn, state)
        true -> aws(conn, state, body)
      end
    end

    defp vault(%{request_path: "/v1/secret/data/" <> path} = conn, state) do
      case state.vault do
        %{^path => fields} -> json(conn, 200, %{"data" => %{"data" => fields, "metadata" => %{}}})
        _missing -> json(conn, 404, %{"errors" => []})
      end
    end

    defp vault(conn, _state), do: json(conn, 404, %{"errors" => []})

    defp aws(conn, state, body) do
      %{"SecretId" => id} = JSON.decode!(body)

      case state.aws do
        %{^id => {:binary, bytes}} ->
          json(conn, 200, %{"SecretBinary" => Base.encode64(bytes)})

        %{^id => value} ->
          json(conn, 200, %{"SecretString" => value})

        _missing ->
          json(conn, 400, %{"__type" => "ResourceNotFoundException", "message" => "nope"})
      end
    end

    defp json(conn, status, term) do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(status, JSON.encode!(term))
    end
  end

  # ── fixtures ────────────────────────────────────────────────────────────────

  setup do
    dir = Path.join(System.tmp_dir!(), "ankusa_secrets_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp write_secret(dir, name, contents) do
    path = Path.join(dir, name)
    File.write!(path, contents)
    path
  end

  defp config_file(yaml) do
    path =
      Path.join(System.tmp_dir!(), "ankusa_secrets_#{System.unique_integer([:positive])}.yml")

    File.write!(path, yaml)
    on_exit(fn -> File.rm(path) end)
    path
  end

  # Sources only; every other key stays at its default.
  defp github(secret_line) do
    """
    sources:
      gh:
        verify:
          type: github
          #{secret_line}
        sinks: [{type: log}]
    """
  end

  defp secret_of(config, id \\ "gh") do
    {Ankusa.SourceStore.Static, opts} = config.source_store
    %{verifier: {_hmac, verifier}} = Ankusa.Source.new(id, opts[:sources][id])
    verifier[:secret]
  end

  defp load!(yaml, opts \\ []),
    do: Config.load!(Keyword.merge([path: config_file(yaml), env: %{}], opts))

  defp error!(yaml, opts \\ []) do
    assert_raise ConfigError, fn -> load!(yaml, opts) end
  end

  defp start_stub(state \\ %{}) do
    agent =
      start_supervised!(
        {Agent, fn -> Map.merge(%{vault: %{}, aws: %{}, down?: false, hits: []}, state) end},
        id: make_ref()
      )

    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)

    start_supervised!(
      {Bandit, plug: {Stub, agent}, scheme: :http, ip: {127, 0, 0, 1}, port: port},
      id: make_ref()
    )

    %{agent: agent, address: "http://127.0.0.1:#{port}"}
  end

  defp stub_set(stub, fun), do: Agent.update(stub.agent, fun)
  defp stub_hits(stub), do: stub.agent |> Agent.get(& &1.hits) |> Enum.reverse()

  defp providers(stub, extra \\ "") do
    """
    secrets:
      #{extra}
      providers:
        vault: {type: vault, address: "#{stub.address}", token: "vault-token-canary"}
        aws:
          type: aws
          region: us-east-1
          endpoint: "#{stub.address}"
          access_key_id: AKIDEXAMPLE
          secret_access_key: aws-secret-canary
    """
  end

  # ── file secret ─────────────────────────────────────────────────────────────

  describe "file secret" do
    test "${file:} reads the file, minus one trailing newline", %{dir: dir} do
      path = write_secret(dir, "whsec", "s3cret-from-file\n")

      assert secret_of(load!(github(~s(secret: "${file:#{path}}"))).config) == "s3cret-from-file"

      crlf = write_secret(dir, "crlf", "windows\r\n")
      assert secret_of(load!(github(~s(secret: "${file:#{crlf}}"))).config) == "windows"

      two = write_secret(dir, "two", "keeps-one\n\n")
      assert secret_of(load!(github(~s(secret: "${file:#{two}}"))).config) == "keeps-one\n"
    end

    test "a reference sits among text and other variables", %{dir: dir} do
      path = write_secret(dir, "mid", "MIDDLE")

      loaded =
        load!(github(~s(secret: "pre-${file:#{path}}-${SUFFIX}")), env: %{"SUFFIX" => "post"})

      assert secret_of(loaded.config) == "pre-MIDDLE-post"
    end

    test "secret_file: is secret: read from a file, one path or a rotation list", %{dir: dir} do
      new = write_secret(dir, "new", "new-secret\n")
      old = write_secret(dir, "old", "old-secret\n")

      assert secret_of(load!(github("secret_file: #{new}")).config) == "new-secret"

      assert secret_of(load!(github("secret_file: [#{new}, #{old}]")).config) ==
               ["new-secret", "old-secret"]

      # the path itself may come from the environment
      loaded = load!(github(~s(secret_file: "${DIR}/new")), env: %{"DIR" => dir})
      assert secret_of(loaded.config) == "new-secret"
    end

    test "secret_file: works on an http sink and a secret list may mix file and variable", %{
      dir: dir
    } do
      sink = write_secret(dir, "sink", "whsec_c2luay1maXh0dXJl")

      loaded =
        load!(
          """
          sources:
            gh:
              verify: {type: github, secret: ["${file:#{sink}}", "${OLD}"]}
              sinks:
                - type: http
                  url: https://sink.example.com/hooks
                  secret_file: #{sink}
          """,
          env: %{"OLD" => "old"}
        )

      {_store, opts} = loaded.config.source_store
      source = Ankusa.Source.new("gh", opts[:sources]["gh"])
      assert [{Ankusa.Sink.Http, http}] = source.sinks
      assert http[:secret] == "whsec_c2luay1maXh0dXJl"
      assert secret_of(loaded.config) == ["whsec_c2luay1maXh0dXJl", "old"]
    end

    test "a file that cannot be used fails the load by dotted key and reference", %{dir: dir} do
      missing = Path.join(dir, "nope")
      error = error!(github(~s(secret: "${file:#{missing}}")))
      assert error.message == "sources.gh.verify.secret: ${file:#{missing}}: file not found"

      error = error!(github("secret_file: [#{write_secret(dir, "a", "a")}, #{missing}]"))

      assert error.message ==
               "sources.gh.verify.secret_file[1]: ${file:#{missing}}: file not found"

      error = error!(github(~s(secret: "${file:#{dir}}")))
      assert error.message =~ "is a directory, not a file"

      big = write_secret(dir, "big", String.duplicate("x", 65_537))
      error = error!(github("secret_file: #{big}"))
      assert error.message =~ "file is larger than 65536 bytes"
      refute error.message =~ "xxxxxxxx"

      empty = write_secret(dir, "empty", "\n")
      error = error!(github("secret_file: #{empty}"))
      assert error.message == "sources.gh.verify.secret: must not be empty"
    end

    test "a relative path, a bad type and both keys at once are refused" do
      assert error!(github(~s(secret: "${file:relative/path}"))).message ==
               "sources.gh.verify.secret: a file secret needs an absolute path, got relative/path"

      assert error!(github("secret_file: relative/path")).message ==
               "sources.gh.verify.secret_file: a file secret needs an absolute path, got relative/path"

      assert error!(github("secret_file: 7")).message ==
               "sources.gh.verify.secret_file: expected a file path or a list of file paths, got 7"

      assert error!(github("secret_file: []")).message ==
               "sources.gh.verify.secret_file: expected a file path or a list of file paths, got []"

      both = github("secret: x\n      secret_file: /tmp/x")

      assert error!(both).message ==
               "sources.gh.verify: secret and secret_file are mutually exclusive"
    end

    test "secret_file: means nothing outside a verifier or an http sink" do
      assert error!("admin: {secret_file: /tmp/x}\n").message ==
               ~s(admin: unknown key "secret_file")
    end

    test "an API-submitted spec cannot name a file of this host" do
      spec = %{
        "verify" => %{"type" => "github", "secret_file" => "/etc/passwd"},
        "sinks" => [%{"type" => "log"}]
      }

      error = assert_raise ConfigError, fn -> Config.source_from_map!("x", spec) end
      assert error.message =~ ~s(unknown key "secret_file")

      literal = put_in(spec, ["verify"], %{"type" => "github", "secret" => "${file:/etc/passwd}"})
      opts = Config.source_from_map!("x", literal)
      assert opts[:verifier] |> elem(1) |> Keyword.fetch!(:secret) == "${file:/etc/passwd}"
    end

    test "an unresolved ${…} names every form" do
      assert error!(github(~s(secret: "${lower_case}"))).message ==
               "sources.gh.verify.secret: unresolved ${…} (use ${NAME}, ${NAME:-default}, " <>
                 "${file:/abs/path} or ${sm:<provider>:<ref>}; variable names are [A-Z0-9_]+; " <>
                 "use ${NAME:-} for an empty default)"
    end

    test "a ${file:} with a variable inside it is not a reference" do
      assert error!(github(~s(secret: "${file:${DIR}/key}")), env: %{"DIR" => "/tmp"}).message =~
               "unresolved ${…}"
    end
  end

  # ── sm ref ──────────────────────────────────────────────────────────────────

  describe "sm ref" do
    test "Vault KV v2 and AWS Secrets Manager references resolve at load" do
      stub =
        start_stub(%{
          vault: %{
            "ankusa/stripe" => %{"whsec" => "vault-whsec", "other" => "x"},
            "ankusa/single" => %{"only" => "vault-single"}
          },
          aws: %{
            "prod/stripe" => ~s({"whsec":"aws-whsec"}),
            "prod/plain" => "aws-plain",
            "prod/binary" => {:binary, "aws-binary"}
          }
        })

      yaml =
        providers(stub) <>
          """
          sources:
            vault:
              verify:
                type: github
                secret: ["${sm:vault:ankusa/stripe#whsec}", "${sm:vault:ankusa/single}"]
              sinks: [{type: log}]
            aws:
              verify:
                type: github
                secret:
                  - "${sm:aws:prod/stripe#whsec}"
                  - "${sm:aws:prod/plain}"
                  - "${sm:aws:prod/binary}"
              sinks: [{type: log}]
          """

      config = load!(yaml).config

      assert secret_of(config, "vault") == ["vault-whsec", "vault-single"]
      assert secret_of(config, "aws") == ["aws-whsec", "aws-plain", "aws-binary"]

      hits = stub_hits(stub)

      assert {"GET", "/v1/secret/data/ankusa/stripe", headers} =
               Enum.find(hits, &(elem(&1, 0) == "GET"))

      assert {"x-vault-token", "vault-token-canary"} in headers

      assert {"POST", "/", headers} = Enum.find(hits, &(elem(&1, 0) == "POST"))
      assert {"x-amz-target", "secretsmanager.GetSecretValue"} in headers
      assert {"content-type", "application/x-amz-json-1.1"} in headers
      {_name, authorization} = List.keyfind(headers, "authorization", 0)

      assert authorization =~
               ~r"^AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/\d{8}/us-east-1/secretsmanager/aws4_request"
    end

    test "a reference used twice is fetched once per load" do
      stub = start_stub(%{vault: %{"ankusa/stripe" => %{"whsec" => "v"}}})

      yaml =
        providers(stub) <>
          """
          sources:
            a: {verify: {type: github, secret: "${sm:vault:ankusa/stripe#whsec}"}, sinks: [{type: log}]}
            b: {verify: {type: github, secret: "${sm:vault:ankusa/stripe#whsec}"}, sinks: [{type: log}]}
          """

      load!(yaml)
      assert length(stub_hits(stub)) == 1
    end

    test "an unreachable, missing or unreadable secret fails the boot, naming key and reference" do
      stub = start_stub(%{vault: %{"ankusa/multi" => %{"a" => "1", "b" => "2"}}})
      ref = fn r -> providers(stub) <> github(~s(secret: "#{r}")) end

      error = error!(ref.("${sm:vault:ankusa/missing#whsec}"))

      assert error.message ==
               "sources.gh.verify.secret: ${sm:vault:ankusa/missing#whsec}: secret not found " <>
                 "(or not readable with the provider's credentials)"

      assert error!(ref.("${sm:vault:ankusa/multi}")).message =~ "secret not found"
      assert error!(ref.("${sm:vault:ankusa/multi#c}")).message =~ "secret not found"
      assert error!(ref.("${sm:aws:prod/missing}")).message =~ "secret not found"
      assert error!(ref.("${sm:vault:a/../b}")).message =~ "secret not found"

      stub_set(stub, &%{&1 | down?: true})
      error = error!(ref.("${sm:vault:ankusa/multi#a}"))

      assert error.message ==
               "sources.gh.verify.secret: ${sm:vault:ankusa/multi#a}: secret provider unavailable"

      for secret <- ["vault-token-canary", "aws-secret-canary"],
          do: refute(error.message =~ secret)

      error = error!(ref.("${sm:aws:prod/anything}"))
      assert error.message =~ "secret provider unavailable"
    end

    test "a provider that is not configured, or is configured wrong, is a load error" do
      stub = start_stub()

      assert error!(github(~s(secret: "${sm:vault:x}"))).message ==
               "sources.gh.verify.secret: ${sm:vault:…} names no provider (secrets.providers has: none)"

      assert error!(providers(stub) <> github(~s(secret: "${sm:nope:x}"))).message ==
               "sources.gh.verify.secret: ${sm:nope:…} names no provider " <>
                 "(secrets.providers has: aws, vault)"

      for {provider, message} <- [
            {"{type: gcp}", "secrets.providers.p.type must be one of aws, vault, got \"gcp\""},
            {"{type: vault, token: t}", ~s(secrets.providers.p: missing required key "address")},
            {"{type: vault, address: \"http://vault.example.com\", token: t}",
             "secrets.providers.p.address must be an https:// URL (http:// is accepted only for a loopback address)"},
            {"{type: aws, region: us-east-1, extra: 1}",
             ~s(secrets.providers.p: unknown key "extra")},
            {"{type: vault, address: \"https://v\", token: t, timeout_ms: 0}",
             "secrets.providers.p.timeout_ms must be an integer in 1..60000, got 0"},
            {"[x]", "secrets.providers.p: expected a mapping, got [\"x\"]"}
          ] do
        yaml = "secrets:\n  providers:\n    p: #{provider}\n" <> github("secret: x")
        assert error!(yaml).message == message
      end

      yaml =
        "secrets:\n  providers:\n    Bad_Name: {type: aws, region: r}\n" <> github("secret: x")

      assert error!(yaml).message =~ "a provider name is [a-z0-9_-]+"

      assert error!("secrets: {cache_ttl_ms: -1}\n" <> github("secret: x")).message ==
               "secrets.cache_ttl_ms must be an integer in 0..86400000, got -1"
    end

    test "a provider's own credentials come from a variable or a file, never from a manager", %{
      dir: dir
    } do
      stub = start_stub(%{vault: %{"p" => %{"v" => "found"}}})
      token = write_secret(dir, "token", "vault-token-from-file\n")

      yaml =
        """
        secrets:
          providers:
            vault: {type: vault, address: "#{stub.address}", token: "${file:#{token}}"}
        """ <> github(~s(secret: "${sm:vault:p#v}"))

      assert secret_of(load!(yaml).config) == "found"
      assert {"x-vault-token", "vault-token-from-file"} in elem(hd(stub_hits(stub)), 2)

      nested =
        "secrets:\n  providers:\n    vault: {type: vault, address: \"#{stub.address}\", " <>
          "token: \"${sm:vault:p#v}\"}\n" <> github("secret: x")

      assert error!(nested).message ==
               "secrets.providers.vault.token: ${sm:vault:…} cannot configure a provider " <>
                 "(use ${VAR} or ${file:/path} for a provider's own credentials)"
    end

    test "cache_ttl_ms reuses a fetched value across loads; refresh: true and ttl 0 do not" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "one"}}})
      yaml = providers(stub, "cache_ttl_ms: 60000") <> github(~s(secret: "${sm:vault:p#v}"))
      path = config_file(yaml)

      first = Config.load!(path: path, env: %{})
      assert secret_of(first.config) == "one"
      assert length(stub_hits(stub)) == 1

      stub_set(stub, &%{&1 | vault: %{"p" => %{"v" => "two"}}})

      cached = Config.load!(path: path, env: %{}, secrets: first.secrets)
      assert secret_of(cached.config) == "one"
      assert length(stub_hits(stub)) == 1

      forced = Config.load!(path: path, env: %{}, secrets: first.secrets, refresh: true)
      assert secret_of(forced.config) == "two"
      assert length(stub_hits(stub)) == 2

      # ttl 0 (the default): every load asks
      path = config_file(providers(stub) <> github(~s(secret: "${sm:vault:p#v}")))
      first = Config.load!(path: path, env: %{})
      Config.load!(path: path, env: %{}, secrets: first.secrets)
      assert length(stub_hits(stub)) == 4
    end

    test "inspecting what a load resolved shows no secret" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "inspect-canary"}}})
      loaded = load!(providers(stub) <> github(~s(secret: "${sm:vault:p#v}")))

      refute inspect(loaded.secrets) =~ "inspect-canary"
      refute inspect(loaded.secrets) =~ "vault-token-canary"
    end
  end

  # ── rotation via reload ─────────────────────────────────────────────────────

  describe "rotation via reload" do
    defp sign(secret, body),
      do: "sha256=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, body), case: :lower)

    # POST `body` signed with `secret` through the instance's edge; the status.
    defp post(instance, secret, source \\ "gh") do
      body = ~s({"zen":"Keep it logically awesome."})

      conn =
        :post
        |> Plug.Test.conn("/webhooks/#{source}", body)
        |> Plug.Conn.put_req_header("x-hub-signature-256", sign(secret, body))
        |> Plug.Conn.put_req_header("x-github-event", "ping")

      Ankusa.Edge.Router.call(conn, Ankusa.Edge.Router.init(instance: instance)).status
    end

    # Boot an edge-only instance from `yaml`, the way the image does: load,
    # remember what the references resolved to, start.
    defp boot(yaml, env \\ %{}) do
      instance = :"secrets_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}")
      on_exit(fn -> File.rm_rf(data_dir) end)

      path =
        config_file(
          "node: {roles: [edge], data_dir: \"#{data_dir}\"}\nadmin: {enabled: false}\n" <> yaml
        )

      loaded = Config.load!(path: path, env: env)

      start_supervised!({Ankusa.Instance, %{loaded.config | instance: instance, port: 0}},
        id: instance
      )

      Reload.remember(instance, loaded)
      %{instance: instance, path: path, env: env}
    end

    defp reload(node, opts \\ []),
      do: Reload.reload(node.instance, [path: node.path, env: node.env] ++ opts)

    test "a secret file swap and a reload verifies with the new secret; the old stays valid until removed",
         %{dir: dir} do
      current = write_secret(dir, "current", "s1\n")
      previous = write_secret(dir, "previous", "s0\n")
      node = boot(github("secret_file: [#{current}, #{previous}]"))

      assert post(node.instance, "s1") == 201
      assert post(node.instance, "s0") == 201
      assert post(node.instance, "s2") == 401

      # the swap: a new current secret lands in the file, the old one moves to the second slot
      File.write!(current, "s2\n")
      File.write!(previous, "s1\n")

      # nothing changes until the reload
      assert post(node.instance, "s2") == 401

      assert {:ok, %{sources: ["gh"], kept: []}} = reload(node)
      assert post(node.instance, "s2") == 201
      assert post(node.instance, "s1") == 201
      assert post(node.instance, "s0") == 401

      # the window closes: the file list drops the old secret
      File.write!(node.path, String.replace(File.read!(node.path), ", #{previous}", ""))
      assert {:ok, %{sources: ["gh"]}} = reload(node)
      assert post(node.instance, "s2") == 201
      assert post(node.instance, "s1") == 401
    end

    test "a reload that changes nothing reports nothing changed", %{dir: dir} do
      file = write_secret(dir, "s", "same")
      node = boot(github("secret_file: #{file}"))

      assert {:ok, %{keys: [], sources: [], kept: []}} = reload(node)
      assert post(node.instance, "same") == 201
    end

    test "a secret manager outage at reload keeps the old secret and warns; recovery rotates it" do
      stub = start_stub(%{vault: %{"ankusa/gh" => %{"whsec" => "v1"}}})
      node = boot(providers(stub) <> github(~s(secret: "${sm:vault:ankusa/gh#whsec}")))
      assert post(node.instance, "v1") == 201

      stub_set(stub, &%{&1 | down?: true, vault: %{"ankusa/gh" => %{"whsec" => "v2"}}})

      log = capture_log(fn -> send(self(), reload(node)) end)
      assert_received {:ok, %{sources: [], kept: [kept]}}

      assert kept == %{path: "sources.gh.verify.secret", reason: "unavailable"}

      assert log =~ "warning"
      assert log =~ "secret reload: ${sm:vault:ankusa/gh#whsec} (sources.gh.verify.secret)"
      assert log =~ "keeping the previous value"
      refute log =~ "vault-token-canary"
      assert post(node.instance, "v1") == 201
      assert post(node.instance, "v2") == 401

      stub_set(stub, &%{&1 | down?: false})
      assert {:ok, %{sources: ["gh"], kept: []}} = reload(node)
      assert post(node.instance, "v2") == 201
      assert post(node.instance, "v1") == 401
    end

    test "a file that is gone at reload keeps its last value", %{dir: dir} do
      file = write_secret(dir, "s", "kept-secret")
      node = boot(github("secret_file: #{file}"))

      File.rm!(file)
      assert {:ok, %{sources: [], kept: [%{reason: "enoent"}]}} = reload(node)
      assert post(node.instance, "kept-secret") == 201
    end

    test "the cache applies to a reload, refresh: true skips it" do
      stub = start_stub(%{vault: %{"p" => %{"v" => "c1"}}})
      node = boot(providers(stub, "cache_ttl_ms: 60000") <> github(~s(secret: "${sm:vault:p#v}")))
      stub_set(stub, &%{&1 | vault: %{"p" => %{"v" => "c2"}}})

      assert {:ok, %{sources: []}} = reload(node)
      assert post(node.instance, "c1") == 201

      assert {:ok, %{sources: ["gh"]}} = reload(node, refresh: true)
      assert post(node.instance, "c2") == 201
      assert post(node.instance, "c1") == 401
    end

    test "a reload that cannot be applied is an error and applies nothing", %{dir: dir} do
      file = write_secret(dir, "s", "stays")
      node = boot(github("secret_file: #{file}"))
      original = File.read!(node.path)

      # a new reference with no previous value cannot be kept
      File.write!(node.path, String.replace(original, file, Path.join(dir, "new-and-missing")))
      assert {:error, {:invalid_config, message}} = reload(node)

      assert message =~
               "sources.gh.verify.secret_file: ${file:#{Path.join(dir, "new-and-missing")}}: file not found"

      assert post(node.instance, "stays") == 201

      # a file that no longer parses
      File.write!(node.path, "sources: [unclosed")
      assert {:error, {:invalid_config, _message}} = reload(node)
      assert post(node.instance, "stays") == 201

      # a source the instance would refuse to boot with
      File.write!(
        node.path,
        github("secret: \"whsec_!!!\"")
        |> String.replace("type: github", "type: standard_webhooks")
      )

      assert {:error, {:invalid_config, message}} = reload(node)
      assert message == "source gh: verifier secret is missing or undecodable"
      assert post(node.instance, "stays") == 201
    end

    test "an instance that is not running is an error" do
      assert {:error, {:invalid_config, "instance :nobody is not running"}} =
               Reload.reload(:nobody, path: config_file(github("secret: x")), env: %{})
    end

    test "a store that keeps its sources elsewhere is told, not silently skipped" do
      instance = :"secrets_persistent_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}")
      on_exit(fn -> File.rm_rf(data_dir) end)

      yaml =
        "node: {roles: [edge], data_dir: \"#{data_dir}\"}\nadmin: {enabled: false}\nsource_store: {type: persistent}\n" <>
          github("secret: x")

      path = config_file(yaml)
      loaded = Config.load!(path: path, env: %{})
      start_supervised!({Ankusa.Instance, %{loaded.config | instance: instance, port: 0}})
      Reload.remember(instance, loaded)

      # the file's sources are seeds for this store: a changed one is reported, and not applied
      File.write!(path, String.replace(yaml, "secret: x", "secret: y"))
      log = capture_log(fn -> send(self(), Reload.reload(instance, path: path, env: %{})) end)
      assert_received {:ok, %{keys: ["sources.gh.verify.secret"], sources: [], kept: []}}
      assert log =~ "does not run source_store.type: static"
    end
  end
end
