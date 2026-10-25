defmodule Ankusa.SDK.HookTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{ClaimCheck, Hook, Message, Webhook}

  defp message(verification) do
    json =
      JSON.encode!(%{
        "v" => 1,
        "id" => "01a0",
        "source_id" => "demo",
        "received_at" => 1_720_000_000_000,
        "size" => 0,
        "body_base64" => ""
      })
      |> JSON.decode!()
      |> then(
        &if verification == :absent, do: &1, else: Map.put(&1, "verification", verification)
      )
      |> JSON.encode!()

    {:ok, message} = Message.decode(json)
    message
  end

  describe "trusted?/1" do
    test "a hook the node verified and did not flag is trusted" do
      assert Hook.trusted?(%Hook{verification: %{status: "ok", flagged: false}})
    end

    test "a flagged hook is untrusted, whatever its status" do
      refute Hook.trusted?(%Hook{verification: %{status: "failed", flagged: true}})
      refute Hook.trusted?(%Hook{verification: %{status: "ok", flagged: true}})
    end

    test "a skipped, failed or unknown status is untrusted" do
      refute Hook.trusted?(%Hook{verification: %{status: "skipped", flagged: false}})
      refute Hook.trusted?(%Hook{verification: %{status: "failed", flagged: false}})
      refute Hook.trusted?(%Hook{verification: %{status: "attested", flagged: false}})
    end

    test "a hook from a node older than the field is untrusted" do
      refute Hook.trusted?(%Hook{})
      refute Hook.trusted?(%Hook{verification: nil})
    end

    test "a decoded message answers the same way" do
      assert Hook.trusted?(message(%{"status" => "ok", "flagged" => false}))
      refute Hook.trusted?(message(%{"status" => "failed", "flagged" => true}))
      refute Hook.trusted?(message(:absent))
    end

    test "a message's hook keeps the verdict across a claim-check conversion" do
      claim_check = ClaimCheck.new("http://gateway.invalid")

      {:ok, flagged} =
        Message.to_hook(message(%{"status" => "failed", "flagged" => true}), claim_check)

      refute Hook.trusted?(flagged)

      {:ok, ok} = Message.to_hook(message(%{"status" => "ok", "flagged" => false}), claim_check)
      assert Hook.trusted?(ok)
    end

    test "parsed headers answer the same way" do
      {:ok, ok} =
        Webhook.parse_headers(%{
          "x-ankusa-id" => "01a0",
          "x-ankusa-verification" => "ok"
        })

      assert Hook.trusted?(ok)

      {:ok, flagged} =
        Webhook.parse_headers(%{
          "x-ankusa-id" => "01a0",
          "x-ankusa-verification" => "failed",
          "x-ankusa-verification-flagged" => "true"
        })

      refute Hook.trusted?(flagged)

      {:ok, legacy} = Webhook.parse_headers(%{"x-ankusa-id" => "01a0"})
      refute Hook.trusted?(legacy)
    end
  end
end
