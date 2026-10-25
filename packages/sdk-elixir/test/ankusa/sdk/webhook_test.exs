defmodule Ankusa.SDK.WebhookTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.Webhook

  describe "parse_headers/1 verification" do
    test "is nil when x-ankusa-verification is absent" do
      assert {:ok, headers} =
               Webhook.parse_headers(%{"x-ankusa-id" => "01a0", "x-ankusa-source" => "demo"})

      assert headers.verification == nil
    end

    test "flagged is true only for the literal string \"true\"" do
      assert {:ok, headers} =
               Webhook.parse_headers(%{
                 "x-ankusa-id" => "01a0",
                 "x-ankusa-verification" => "failed",
                 "x-ankusa-verification-flagged" => "true"
               })

      assert headers.verification == %{status: "failed", flagged: true}

      assert {:ok, other_value} =
               Webhook.parse_headers(%{
                 "x-ankusa-id" => "01a0",
                 "x-ankusa-verification" => "ok",
                 "x-ankusa-verification-flagged" => "yes"
               })

      assert other_value.verification == %{status: "ok", flagged: false}
    end
  end

  describe "parse_headers/1 delivery metadata" do
    @sha256 "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    test "reads received_at as an integer and body_sha256 as is" do
      assert {:ok, headers} =
               Webhook.parse_headers([
                 {"X-Ankusa-Id", "01a0"},
                 {"x-ankusa-received-at", "1720000000000"},
                 {"x-ankusa-body-sha256", @sha256}
               ])

      assert headers.received_at == 1_720_000_000_000
      assert headers.body_sha256 == @sha256
    end

    test "an absent, empty or malformed value is nil, never an error" do
      for {received_at, sha256} <- [
            {nil, nil},
            {"", ""},
            {"-1", String.upcase(@sha256)},
            {"1720000000000000", binary_part(@sha256, 0, 63)}
          ] do
        headers =
          %{"x-ankusa-id" => "01a0"}
          |> then(&if received_at, do: Map.put(&1, "x-ankusa-received-at", received_at), else: &1)
          |> then(&if sha256, do: Map.put(&1, "x-ankusa-body-sha256", sha256), else: &1)

        assert {:ok, parsed} = Webhook.parse_headers(headers)
        assert parsed.received_at == nil
        assert parsed.body_sha256 == nil
      end
    end
  end
end
