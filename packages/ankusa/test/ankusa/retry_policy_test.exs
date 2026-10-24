defmodule Ankusa.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Ankusa.{RetryPolicy, Source}
  alias Ankusa.RetryPolicy.Exponential

  describe "normalize_opts!/2" do
    test "nil is no override; a keyword list or a map of the known keys is a keyword list" do
      assert RetryPolicy.normalize_opts!(nil, "source") == nil
      assert RetryPolicy.normalize_opts!([], "source") == []

      assert RetryPolicy.normalize_opts!(
               %{max_elapsed_ms: 5_000, base_ms: 0, max_ms: 1, max_attempts: nil, jitter: false},
               "source"
             )
             |> Enum.sort() ==
               [base_ms: 0, jitter: false, max_attempts: nil, max_elapsed_ms: 5_000, max_ms: 1]
    end

    test "an unknown key, a value out of range, or a wrong shape raises naming the source" do
      assert_raise ArgumentError, ~r/source "a": unknown retry key :max_elapsed/, fn ->
        RetryPolicy.normalize_opts!([max_elapsed: 1], ~s(source "a"))
      end

      assert_raise ArgumentError,
                   ~r/source "a": retry.max_elapsed_ms must be an integer from 1000 to 2592000000, got 999/,
                   fn -> RetryPolicy.normalize_opts!([max_elapsed_ms: 999], ~s(source "a")) end

      assert_raise ArgumentError, ~r/source "a": retry must be a keyword list or a map/, fn ->
        RetryPolicy.normalize_opts!("24h", ~s(source "a"))
      end
    end
  end

  describe "Ankusa.Source" do
    test "retry defaults to nil and is validated when the source is built" do
      assert %Source{retry: nil} = Source.new("a", sinks: [{Ankusa.Sink.Log, []}])

      assert %Source{retry: [max_elapsed_ms: 60_000]} =
               Source.new("a", retry: %{max_elapsed_ms: 60_000})

      assert_raise ArgumentError, ~r/source "a": retry.max_elapsed_ms must be an integer/, fn ->
        Source.new("a", retry: [max_elapsed_ms: 0])
      end

      assert_raise ArgumentError, ~r/source "a": unknown retry key :attempts/, fn ->
        Source.new("a", retry: [attempts: 3])
      end
    end
  end

  describe "for_source/2" do
    test "lays a source's options over the instance's, keeping the module" do
      instance = {Exponential, [base_ms: 100, max_elapsed_ms: 1_000_000]}

      assert RetryPolicy.for_source(instance, nil) == instance

      assert {Exponential, opts} =
               RetryPolicy.for_source(instance, max_elapsed_ms: 5_000, jitter: false)

      assert opts[:base_ms] == 100
      assert opts[:max_elapsed_ms] == 5_000
      assert opts[:jitter] == false
    end
  end
end
