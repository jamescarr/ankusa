defmodule Ankusa.SDK.MessageTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{
    ClaimCheck,
    ClaimIntegrityError,
    Hook,
    InvalidMessageError,
    Message,
    PlugTransport
  }

  @ref "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002"
  @body ~s({"id":"evt_1"})
  @sha256 Base.encode16(:crypto.hash(:sha256, @body), case: :lower)

  defp inline_message(overrides \\ %{}) do
    Map.merge(
      %{
        "v" => 1,
        "id" => "01a0",
        "source_id" => "stripe",
        "tenant_id" => "acme",
        "received_at" => 1_737_500_000_000,
        "content_type" => "application/json",
        "size" => byte_size(@body),
        "body_base64" => Base.encode64(@body)
      },
      overrides
    )
  end

  defp claim_message(overrides \\ %{}) do
    Map.merge(
      %{
        "v" => 1,
        "id" => "01a0",
        "source_id" => "stripe",
        "tenant_id" => "acme",
        "received_at" => 1_737_500_000_000,
        "content_type" => "application/json",
        "size" => 4_194_304,
        "claim" => @ref,
        "sha256" => @sha256
      },
      overrides
    )
  end

  defp decode(message), do: Message.decode(JSON.encode!(message))

  describe "decode/1" do
    test "decodes an inline message" do
      assert {:ok, message} = decode(inline_message())

      assert message.v == 1
      assert message.id == "01a0"
      assert message.source_id == "stripe"
      assert message.tenant_id == "acme"
      assert message.received_at == 1_737_500_000_000
      assert message.content_type == "application/json"
      assert message.size == byte_size(@body)
      assert message.body == @body
      assert message.claim == nil
      assert message.sha256 == nil
    end

    test "decodes a claim message" do
      assert {:ok, message} = decode(claim_message())

      assert message.body == nil
      assert message.claim == @ref
      assert message.sha256 == @sha256
      assert message.size == 4_194_304
    end

    test "ignores keys it does not know" do
      assert {:ok, message} = decode(inline_message(%{"future_field" => %{"nested" => true}}))
      assert message.body == @body
    end

    test "accepts null and absent tenant_id and content_type" do
      assert {:ok, nulled} = decode(inline_message(%{"tenant_id" => nil, "content_type" => nil}))
      assert nulled.tenant_id == nil
      assert nulled.content_type == nil

      assert {:ok, absent} =
               decode(inline_message() |> Map.drop(["tenant_id", "content_type"]))

      assert absent.tenant_id == nil
      assert absent.content_type == nil
    end

    test "an empty content_type is a value, not an absence" do
      assert {:ok, message} = decode(inline_message(%{"content_type" => ""}))
      assert message.content_type == ""
    end

    test "rejects a body that is not JSON" do
      assert {:error, %InvalidMessageError{reason: :invalid_json}} = Message.decode("{nope")
    end

    test "rejects JSON that is not an object" do
      assert {:error, %InvalidMessageError{reason: :not_an_object}} = Message.decode("[1,2]")
    end

    test "rejects a version it does not speak" do
      assert {:error, %InvalidMessageError{reason: {:unsupported_version, 2}}} =
               decode(inline_message(%{"v" => 2}))

      assert {:error, %InvalidMessageError{reason: {:unsupported_version, nil}}} =
               decode(Map.delete(inline_message(), "v"))
    end

    test "rejects a malformed field, naming it" do
      assert {:error, %InvalidMessageError{reason: {:invalid_field, "id"}}} =
               decode(inline_message(%{"id" => ""}))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "source_id"}}} =
               decode(Map.delete(inline_message(), "source_id"))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "tenant_id"}}} =
               decode(inline_message(%{"tenant_id" => 5}))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "received_at"}}} =
               decode(inline_message(%{"received_at" => "now"}))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "content_type"}}} =
               decode(inline_message(%{"content_type" => 5}))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "size"}}} =
               decode(inline_message(%{"size" => -1}))
    end

    test "rejects a message that is both inline and claimed" do
      message = claim_message(%{"body_base64" => Base.encode64(@body)})
      assert {:error, %InvalidMessageError{reason: :ambiguous_body}} = decode(message)
    end

    test "rejects a body_base64 that is not base64" do
      assert {:error, %InvalidMessageError{reason: :invalid_body_base64}} =
               decode(inline_message(%{"body_base64" => "!!!"}))
    end

    test "rejects a claim without its sha256" do
      assert {:error, %InvalidMessageError{reason: {:invalid_field, "sha256"}}} =
               decode(Map.delete(claim_message(), "sha256"))

      assert {:error, %InvalidMessageError{reason: {:invalid_field, "sha256"}}} =
               decode(claim_message(%{"sha256" => nil}))
    end

    test "rejects a message with no body at all" do
      assert {:error, %InvalidMessageError{reason: :missing_body}} =
               decode(Map.drop(inline_message(), ["body_base64"]))
    end
  end

  describe "decode/1 integrity and optional fields" do
    test "carries dedupe_key, replay_id, and headers" do
      assert {:ok, message} =
               decode(
                 inline_message(%{
                   "dedupe_key" => "evt_1",
                   "replay_id" => "rid-1",
                   "headers" => %{"x-github-event" => "push"},
                   "sha256" => @sha256
                 })
               )

      assert message.dedupe_key == "evt_1"
      assert message.replay_id == "rid-1"
      assert message.headers == %{"x-github-event" => "push"}
      assert message.sha256 == @sha256
    end

    test "absent optional fields decode to null and an empty headers map" do
      assert {:ok, message} = decode(inline_message())

      assert message.dedupe_key == nil
      assert message.replay_id == nil
      assert message.sha256 == nil
      assert message.headers == %{}
    end

    test "rejects headers whose values are not all strings, naming headers" do
      assert {:error, %InvalidMessageError{code: "invalid_field", field: "headers"}} =
               decode(inline_message(%{"headers" => %{"x-n" => 42}}))
    end

    test "rejects a malformed sha256 at the field stage" do
      assert {:error, %InvalidMessageError{code: "invalid_field", field: "sha256"}} =
               decode(inline_message(%{"sha256" => "not-hex"}))
    end

    test "a decoded length other than size is a size_mismatch" do
      assert {:error, %InvalidMessageError{code: "size_mismatch", retryable: false}} =
               decode(inline_message(%{"size" => byte_size(@body) + 1}))
    end

    test "a digest that does not match the body is an integrity error" do
      assert {:error, %InvalidMessageError{code: "integrity", retryable: false}} =
               decode(inline_message(%{"sha256" => String.duplicate("a", 64)}))
    end

    test "a matching inline digest decodes" do
      assert {:ok, message} = decode(inline_message(%{"sha256" => @sha256}))
      assert message.body == @body
    end

    test "rejects a claim that does not parse as a claim ref" do
      assert {:error, %InvalidMessageError{code: "invalid_field", field: "claim"}} =
               decode(claim_message(%{"claim" => "not-a-ref"}))
    end

    test "rejects a claim whose tenant disagrees with tenant_id" do
      assert {:error, %InvalidMessageError{code: "tenant_mismatch", retryable: false}} =
               decode(claim_message(%{"tenant_id" => "other"}))
    end

    test "a claim whose tenant matches, or a null tenant, decodes" do
      assert {:ok, _message} = decode(claim_message())
      assert {:ok, _message} = decode(claim_message(%{"tenant_id" => nil}))
    end
  end

  describe "to_hook/2" do
    test "an inline message makes no request" do
      plug = fn _conn -> flunk("an inline message must not reach the claim check") end
      claim_check = ClaimCheck.new("http://gateway.invalid", req_options: [plug: plug])
      {:ok, message} = decode(inline_message())

      assert {:ok, %Hook{} = hook} = Message.to_hook(message, claim_check)

      assert hook.id == "01a0"
      assert hook.source_id == "stripe"
      assert hook.tenant_id == "acme"
      assert hook.content_type == "application/json"
      assert hook.body == @body
      assert hook.received_at == 1_737_500_000_000
      assert hook.size == byte_size(@body)
    end

    test "an inline message carries its dedupe key, replay id, and headers onto the hook" do
      plug = fn _conn -> flunk("an inline message must not reach the claim check") end
      claim_check = ClaimCheck.new("http://gateway.invalid", req_options: [plug: plug])

      {:ok, message} =
        decode(
          inline_message(%{
            "dedupe_key" => "evt_1",
            "replay_id" => "rid-1",
            "headers" => %{"x-github-event" => "push"}
          })
        )

      assert {:ok, %Hook{} = hook} = Message.to_hook(message, claim_check)
      assert hook.dedupe_key == "evt_1"
      assert hook.replay_id == "rid-1"
      assert hook.headers == %{"x-github-event" => "push"}
    end

    test "a claim message redeems it through the gateway" do
      {plug, recorder} =
        PlugTransport.transport(fn conn -> PlugTransport.text(conn, 200, @body) end)

      claim_check = ClaimCheck.new("http://gateway.invalid", req_options: [plug: plug])
      {:ok, message} = decode(claim_message())

      assert {:ok, hook} = Message.to_hook(message, claim_check)
      assert hook.body == @body
      assert hook.size == 4_194_304

      assert [request] = PlugTransport.requests(recorder)
      assert request["method"] == "GET"
      assert request["path"] == "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"
      assert request["body"] == nil
    end

    test "a claim message whose bytes do not match the sha256 is an integrity error" do
      {plug, _recorder} =
        PlugTransport.transport(fn conn -> PlugTransport.text(conn, 200, "tampered") end)

      claim_check = ClaimCheck.new("http://gateway.invalid", req_options: [plug: plug])
      {:ok, message} = decode(claim_message())

      assert {:error, %ClaimIntegrityError{retryable: false}} =
               Message.to_hook(message, claim_check)
    end
  end
end
