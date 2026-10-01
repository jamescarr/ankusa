defmodule Ankusa.SDK.HTTPTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{Admin, AdminUnavailableError, ClaimCheck, ConformanceGateway, Recorder}

  # Req refuses `:finch` together with `:connect_options`, and the SDK always
  # used to set `:connect_options` from `timeout_ms`: any request through a
  # caller's pool raised before it was sent.
  describe "a caller-supplied Finch pool" do
    setup do
      pool = :"ankusa_sdk_finch_#{System.unique_integer([:positive])}"
      start_supervised!({Finch, name: pool})
      %{pool: pool}
    end

    test "carries a real request", %{pool: pool} do
      recorder = Recorder.new()

      url =
        ConformanceGateway.start(
          %{
            "status" => 200,
            "headers" => %{"content-type" => "application/json"},
            "body" => %{"json" => %{"status" => "ok"}}
          },
          recorder
        )

      admin = Admin.new(url, req_options: [finch: [name: pool]])

      assert {:ok, %{"status" => "ok"}} = Admin.health(admin)
      assert [%{"method" => "GET", "path" => "/health"}] = Recorder.requests(recorder)
    end

    test "timeout_ms still bounds the response", %{pool: pool} do
      recorder = Recorder.new()

      url =
        ConformanceGateway.start(
          %{"status" => 200, "body" => %{"json" => %{"status" => "ok"}}, "delay_ms" => 1_000},
          recorder
        )

      admin = Admin.new(url, timeout_ms: 200, req_options: [finch: [name: pool]])

      assert {:error, %AdminUnavailableError{reason: %Req.TransportError{reason: :timeout}}} =
               Admin.health(admin)
    end
  end

  test "new/2 refuses :finch together with :connect_options" do
    assert_raise ArgumentError, ~r/cannot combine :finch and :connect_options/, fn ->
      ClaimCheck.new("http://gateway.invalid",
        req_options: [finch: [name: SomePool], connect_options: [timeout: 1_000]]
      )
    end
  end
end
