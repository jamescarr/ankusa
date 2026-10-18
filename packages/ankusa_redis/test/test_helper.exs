ExUnit.start()

# `Ankusa.RedisLifecycleHarness`: the child-BEAM harness behind
# `redis_lifecycle_test.exs`.
Code.require_file("support/lifecycle_harness.exs", __DIR__)
