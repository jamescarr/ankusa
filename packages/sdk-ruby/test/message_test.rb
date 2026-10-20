# frozen_string_literal: true

require "test_helper"

class MessageTest < Minitest::Test
  BASE = {
    v: 1, id: "01a0", source_id: "demo", received_at: 1720000000000, size: 0, body_base64: ""
  }.freeze

  def test_decode_message_verification_absent_is_nil
    message = Ankusa.decode_message(JSON.generate(BASE))

    assert_nil message.verification
  end

  def test_decode_message_verification_invalid_field_raises
    payload = BASE.merge(verification: {flagged: true})

    error = assert_raises(Ankusa::InvalidMessageError) { Ankusa.decode_message(JSON.generate(payload)) }
    assert_equal "invalid_field", error.code
    assert_equal "verification", error.field
  end
end
