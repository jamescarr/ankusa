# frozen_string_literal: true

require "test_helper"

class WebhookTest < Minitest::Test
  def test_parse_headers_verification_absent_is_nil
    headers = Ankusa.parse_headers({"x-ankusa-id" => "01a0", "x-ankusa-source" => "demo"})

    assert_nil headers.verification
  end

  def test_parse_headers_verification_flagged_header_is_true
    headers = Ankusa.parse_headers(
      "x-ankusa-id" => "01a0",
      "x-ankusa-source" => "demo",
      "x-ankusa-verification" => "failed",
      "x-ankusa-verification-flagged" => "true"
    )

    refute_nil headers.verification
    assert_equal "failed", headers.verification.status
    assert headers.verification.flagged
  end

  def test_trusted_is_true_only_for_an_unflagged_ok
    ok = Ankusa.parse_headers("x-ankusa-id" => "01a0", "x-ankusa-verification" => "ok")
    flagged = Ankusa.parse_headers(
      "x-ankusa-id" => "01a0",
      "x-ankusa-verification" => "failed",
      "x-ankusa-verification-flagged" => "true"
    )
    skipped = Ankusa.parse_headers("x-ankusa-id" => "01a0", "x-ankusa-verification" => "skipped")
    legacy = Ankusa.parse_headers("x-ankusa-id" => "01a0")

    assert_predicate ok, :trusted?
    refute_predicate flagged, :trusted?
    refute_predicate skipped, :trusted?
    refute_predicate legacy, :trusted?
  end
end
