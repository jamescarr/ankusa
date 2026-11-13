defmodule AnkusaGatewayServer.LogFormatterTest do
  @moduledoc "`log.format: json` (`AnkusaGatewayServer.LogFormatter`): one JSON object per line, secrets redacted."

  use ExUnit.Case, async: true

  alias AnkusaGatewayServer.LogFormatter

  test "a formatted line is one JSON object, the values of secret keys redacted" do
    event = %{
      level: :warning,
      msg: {:string, "gateway refused a token"},
      meta: %{
        time: 1_761_300_000_123_456,
        instance: :default,
        authorization: "Bearer t-1",
        nested: %{client_secret: "s-1", issuer: "https://idp.test/"}
      }
    }

    raw = event |> LogFormatter.format(%{instance: nil}) |> IO.iodata_to_binary()

    assert String.ends_with?(raw, "}\n")
    assert [_one] = String.split(raw, "\n", trim: true)

    assert %{
             "level" => "warning",
             "msg" => "gateway refused a token",
             "instance" => "default",
             "authorization" => "[REDACTED]",
             "nested" => %{"client_secret" => "[REDACTED]", "issuer" => "https://idp.test/"}
           } = JSON.decode!(raw)

    refute raw =~ "t-1"
    refute raw =~ "s-1"
  end
end
