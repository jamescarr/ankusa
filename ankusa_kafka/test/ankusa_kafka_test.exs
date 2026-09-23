defmodule AnkusaKafkaTest do
  use ExUnit.Case
  doctest AnkusaKafka

  test "greets the world" do
    assert AnkusaKafka.hello() == :world
  end
end
