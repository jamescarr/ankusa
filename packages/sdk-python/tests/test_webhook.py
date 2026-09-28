import pytest

from ankusa import HookHeaders, MissingHookIdError, parse_headers


def test_parses_a_full_set_of_headers() -> None:
    headers = {
        "x-ankusa-id": "01a0",
        "x-ankusa-source": "demo",
        "x-ankusa-seq": "7",
        "x-ankusa-tenant": "acme",
        "content-type": "application/json",
    }
    assert parse_headers(headers) == HookHeaders(
        id="01a0", source="demo", seq=7, tenant="acme", content_type="application/json"
    )


def test_lookup_is_case_insensitive() -> None:
    headers = {"X-Ankusa-Id": "01a0", "X-Ankusa-Source": "demo"}
    parsed = parse_headers(headers)
    assert parsed.id == "01a0"
    assert parsed.source == "demo"


def test_tenant_and_seq_default_to_none_when_absent() -> None:
    parsed = parse_headers({"x-ankusa-id": "01a0"})
    assert parsed.tenant is None
    assert parsed.seq is None
    assert parsed.source == ""


def test_raises_missing_hook_id_error_without_x_ankusa_id() -> None:
    with pytest.raises(MissingHookIdError):
        parse_headers({"x-ankusa-source": "demo"})


def test_non_numeric_seq_is_none_rather_than_raising() -> None:
    parsed = parse_headers({"x-ankusa-id": "01a0", "x-ankusa-seq": "not-a-number"})
    assert parsed.seq is None
