import urllib.request
import urllib.error
import json

API_BASE = "http://localhost:8080"


def test_cancel():
    # 1. Init upload
    print("=== Test: Cancel Upload ===")
    req = urllib.request.Request(
        f"{API_BASE}/uploads/init",
        data=json.dumps({"mimeType": "text/plain", "expectedSize": 12, "partsCount": 1}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    res = urllib.request.urlopen(req)
    data = json.loads(res.read())
    session_id = data["sessionId"]
    print(f"[1/4] Session initialized: {session_id}")

    # 2. Cancel
    cancel_req = urllib.request.Request(
        f"{API_BASE}/uploads/{session_id}/cancel",
        method="POST",
    )
    cancel_res = urllib.request.urlopen(cancel_req)
    assert cancel_res.status == 200, f"Expected 200, got {cancel_res.status}"
    print(f"[2/4] Cancel returned {cancel_res.status}")

    # 3. Verify session is ABANDONED
    poll_req = urllib.request.Request(f"{API_BASE}/uploads/{session_id}")
    poll_res = urllib.request.urlopen(poll_req)
    poll_data = json.loads(poll_res.read())
    assert poll_data["status"] == "ABANDONED", f"Expected ABANDONED, got {poll_data['status']}"
    print(f"[3/4] Session status is ABANDONED: OK")

    # 4. Try completing the cancelled session — should fail
    try:
        complete_req = urllib.request.Request(
            f"{API_BASE}/uploads/{session_id}/complete",
            data=json.dumps({"parts": [{"partNumber": 1, "etag": '"dummy"', "sizeBytes": 0}]}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(complete_req)
        print("[4/4] FAILED: Complete should have rejected cancelled session")
        exit(1)
    except urllib.error.HTTPError as e:
        assert e.code == 500, f"Expected 500, got {e.code}"
        print(f"[4/4] Complete rejected cancelled session with HTTP {e.code}: OK")

    print("\n✅ Cancel test PASSED!")


def test_cancel_invalid_id():
    print("\n=== Test: Cancel Invalid UUID ===")
    try:
        req = urllib.request.Request(
            f"{API_BASE}/uploads/not-a-uuid/cancel",
            method="POST",
        )
        urllib.request.urlopen(req)
        print("FAILED: Expected 400 for invalid UUID")
        exit(1)
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"
        print(f"Invalid UUID returns {e.code}: OK")


def test_cancel_not_found():
    print("\n=== Test: Cancel Non-Existent Session ===")
    try:
        req = urllib.request.Request(
            f"{API_BASE}/uploads/00000000-0000-0000-0000-000000000000/cancel",
            method="POST",
        )
        urllib.request.urlopen(req)
        print("FAILED: Expected 404 for non-existent session")
        exit(1)
    except urllib.error.HTTPError as e:
        assert e.code == 404, f"Expected 404, got {e.code}"
        print(f"Non-existent session returns {e.code}: OK")


if __name__ == "__main__":
    test_cancel_invalid_id()
    test_cancel_not_found()
    test_cancel()
