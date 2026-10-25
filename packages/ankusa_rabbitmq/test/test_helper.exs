ExUnit.start()

# The toxiproxy fault suite (DEL-7) and the helpers it shares with every adapter.
Code.require_file("../../ankusa/test/support/broker_faults.exs", __DIR__)
