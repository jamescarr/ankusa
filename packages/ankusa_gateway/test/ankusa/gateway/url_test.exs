defmodule Ankusa.Gateway.UrlTest do
  @moduledoc """
  The one scheme rule every outbound endpoint obeys (`Ankusa.Gateway.Url`),
  and the decision points' literal-URL rule on top of it
  (`Ankusa.Gateway.Authz.Remote.url!/3`).
  """

  use ExUnit.Case, async: true

  alias Ankusa.Gateway.Authz.Remote
  alias Ankusa.Gateway.Url

  test "https anywhere, http only to this machine unless allowed" do
    for {url, plain, insecure} <- [
          {"https://idp.example/introspect", :ok, :ok},
          {"http://localhost:8080/x", :ok, :ok},
          {"http://localhost./x", :ok, :ok},
          {"http://pdp.localhost/x", :ok, :ok},
          {"http://127.0.0.1:9000/x", :ok, :ok},
          {"http://127.10.0.1/x", :ok, :ok},
          {"http://[::1]:9000/x", :ok, :ok},
          {"http://pdp.example/x", :error, :ok},
          {"http://localhost.example/x", :error, :ok},
          {"ftp://pdp.example/x", :error, :error},
          {"https:///x", :error, :error},
          {"pdp.example", :error, :error}
        ] do
      assert outcome(fn -> Url.https!(url, "k.url") end) == plain, url

      assert outcome(fn -> Url.https!(url, "k.url", allow_insecure_http: true) end) == insecure,
             url
    end
  end

  test "a decision point's URL is that rule, and literal" do
    assert Remote.url!("https://pdp.example", "a") == "https://pdp.example"

    assert Remote.url!("http://pdp.example", "a", allow_insecure_http: true) ==
             "http://pdp.example"

    assert_raise ArgumentError, ~r/^a\.url must be an https URL.*allow_insecure_http/, fn ->
      Remote.url!("http://pdp.example", "a")
    end

    assert_raise ArgumentError, ~r/^a\.url must be a literal URL/, fn ->
      Remote.url!("https://pdp.example/{{ principal.id }}", "a")
    end
  end

  defp outcome(fun) do
    fun.()
    :ok
  rescue
    ArgumentError -> :error
  end
end
