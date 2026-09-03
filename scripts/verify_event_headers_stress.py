"""Standalone Adversarial Verification Harness for event_headers.

Empirically stress-tests the event_headers boundary function in
src/lab28_platform/integration_tasks.py across diverse inputs, edge cases,
byte encodings, and contract constraints.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lab28_platform.integration_tasks import event_headers  # noqa: E402

W3C_VALID_SAMPLE = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
W3C_UNSAMPLED = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
W3C_ALL_ZEROS = "00-00000000000000000000000000000000-0000000000000000-00"


def test_none_traceparent() -> None:
    """None traceparent must yield exactly 1 tuple containing idempotency-key only."""
    res = event_headers(None, "key-42")
    assert isinstance(res, list), f"Expected list, got {type(res)}"
    assert len(res) == 1, f"Expected 1 header, got {len(res)}: {res}"
    k, v = res[0]
    assert k == "idempotency-key", f"Expected 'idempotency-key', got {k}"
    assert isinstance(k, str), f"Key must be str, got {type(k)}"
    assert v == b"key-42", f"Expected b'key-42', got {v}"
    assert isinstance(v, bytes), f"Value must be bytes, got {type(v)}"
    assert dict(res) == {"idempotency-key": b"key-42"}
    print("  [PASS] test_none_traceparent")


def test_empty_string_traceparent() -> None:
    """Empty string traceparent must yield exactly 1 tuple containing idempotency-key only."""
    res = event_headers("", "key-empty-tp")
    assert isinstance(res, list), f"Expected list, got {type(res)}"
    assert len(res) == 1, f"Expected 1 header, got {len(res)}: {res}"
    k, v = res[0]
    assert k == "idempotency-key", f"Expected 'idempotency-key', got {k}"
    assert isinstance(k, str), f"Key must be str, got {type(k)}"
    assert v == b"key-empty-tp", f"Expected b'key-empty-tp', got {v}"
    assert isinstance(v, bytes), f"Value must be bytes, got {type(v)}"
    assert "traceparent" not in dict(res), "traceparent must NOT be present for empty string"
    assert dict(res) == {"idempotency-key": b"key-empty-tp"}
    print("  [PASS] test_empty_string_traceparent")


def test_valid_w3c_traceparent() -> None:
    """Valid W3C traceparents must yield exactly 2 tuples in deterministic order."""
    for tp in [W3C_VALID_SAMPLE, W3C_UNSAMPLED, W3C_ALL_ZEROS]:
        res = event_headers(tp, "feedback:101")
        assert isinstance(res, list), f"Expected list, got {type(res)}"
        assert len(res) == 2, f"Expected 2 headers for {tp}, got {len(res)}"
        
        # Check first tuple: idempotency-key
        k0, v0 = res[0]
        assert k0 == "idempotency-key"
        assert isinstance(k0, str)
        assert v0 == b"feedback:101"
        assert isinstance(v0, bytes)
        
        # Check second tuple: traceparent
        k1, v1 = res[1]
        assert k1 == "traceparent"
        assert isinstance(k1, str)
        assert v1 == tp.encode(), f"Byte encoding mismatch: {v1} vs {tp.encode()}"
        assert isinstance(v1, bytes)
        
        # Check dict conversion
        d = dict(res)
        assert d == {
            "idempotency-key": b"feedback:101",
            "traceparent": tp.encode(),
        }
    print("  [PASS] test_valid_w3c_traceparent (sampled, unsampled, boundary zeros)")


def test_unicode_and_special_characters() -> None:
    """Byte encoding must properly handle UTF-8 multi-byte characters and symbols."""
    unicode_keys = [
        "yêu-cầu-xác-thực:12345",
        "🚀-ai-platform-event-999",
        "key/with/slashes/and:colons?query=1&b=2",
        "special!@#$%^&*()_+~`",
    ]
    for key in unicode_keys:
        res = event_headers(W3C_VALID_SAMPLE, key)
        assert res[0] == ("idempotency-key", key.encode("utf-8"))
        assert res[1] == ("traceparent", W3C_VALID_SAMPLE.encode("utf-8"))
        assert isinstance(res[0][1], bytes)
        assert isinstance(res[1][1], bytes)
    print("  [PASS] test_unicode_and_special_characters")


def test_caller_mutation_isolation() -> None:
    """Mutating the returned list must not affect subsequent calls."""
    res1 = event_headers(None, "key-iso")
    res1.append(("extra-header", b"malicious"))
    res1[0] = ("idempotency-key", b"tampered")
    
    res2 = event_headers(None, "key-iso")
    assert len(res2) == 1
    assert res2[0] == ("idempotency-key", b"key-iso")
    print("  [PASS] test_caller_mutation_isolation")


def test_boundary_empty_idempotency_key() -> None:
    """Empty idempotency key should still encode cleanly to b''."""
    res = event_headers(None, "")
    assert res == [("idempotency-key", b"")]
    print("  [PASS] test_boundary_empty_idempotency_key")


def test_stress_and_performance() -> None:
    """Verify performance over 50,000 invocations with alternating inputs."""
    start = time.perf_counter()
    n = 50_000
    for i in range(n):
        if i % 3 == 0:
            res = event_headers(None, f"key-{i}")
            assert len(res) == 1
        elif i % 3 == 1:
            res = event_headers("", f"key-{i}")
            assert len(res) == 1
        else:
            res = event_headers(W3C_VALID_SAMPLE, f"key-{i}")
            assert len(res) == 2
    duration = time.perf_counter() - start
    rate = n / duration
    print(
        f"  [PASS] test_stress_and_performance: {n} calls in "
        f"{duration:.3f}s ({rate:,.0f} ops/sec)"
    )


def test_large_payload_key() -> None:
    """Stress test with 64KB idempotency key string."""
    large_key = "k" * 65536
    res = event_headers(W3C_VALID_SAMPLE, large_key)
    assert len(res) == 2
    assert len(res[0][1]) == 65536
    assert res[0][1] == large_key.encode()
    print("  [PASS] test_large_payload_key (64KB key)")


def main() -> None:
    print("=== RUNNING ADVERSARIAL STRESS TEST HARNESS: event_headers ===")
    test_none_traceparent()
    test_empty_string_traceparent()
    test_valid_w3c_traceparent()
    test_unicode_and_special_characters()
    test_caller_mutation_isolation()
    test_boundary_empty_idempotency_key()
    test_stress_and_performance()
    test_large_payload_key()
    print("=== ALL 8 STRESS TEST SUITES PASSED SUCCESSFULLY ===")


if __name__ == "__main__":
    main()
