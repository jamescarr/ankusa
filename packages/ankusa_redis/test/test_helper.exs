ExUnit.start()

# `Ankusa.RedisLifecycleHarness`: the child-BEAM harness behind
# `redis_lifecycle_test.exs`.
Code.require_file("support/lifecycle_harness.exs", __DIR__)

# The toxiproxy fault suite (DEL-7) and the helpers it shares with every adapter.
Code.require_file("../../ankusa/test/support/broker_faults.exs", __DIR__)
