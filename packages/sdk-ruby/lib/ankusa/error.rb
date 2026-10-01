# frozen_string_literal: true

module Ankusa
  # The root of every error this SDK raises; each client's family base (and so
  # every error below it) is a subclass. It deliberately does not respond to
  # `retryable?`: only a family that defines the retryable/not-retryable split
  # carries that bit, so a caller can't read one off this class by mistake.
  class Error < StandardError
  end
end
