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
end
