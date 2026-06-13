import json
import mimetypes
import os
import sys
import time
import urllib.request

API_BASE = "http://localhost:8080"


def run_test_for_file(filepath):
    filename = os.path.basename(filepath)
    size = os.path.getsize(filepath)
    mime_type, _ = mimetypes.guess_type(filepath)
    if not mime_type:
        mime_type = "application/octet-stream"

    print(f"\n=========================================")
    print(f"🧪 Testing file: {filename} ({mime_type}, {size} bytes)")
    print(f"=========================================")

    # 1. Init
    init_req = urllib.request.Request(
        f"{API_BASE}/uploads/init",
        data=json.dumps(
            {"mimeType": mime_type, "expectedSize": size, "partsCount": 1}
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    init_res = urllib.request.urlopen(init_req)
    init_data = json.loads(init_res.read())
    session_id = init_data["sessionId"]
    upload_url = init_data["parts"]["1"]
    print(f"[1/5] Session initialized: {session_id}")

    # 2. Upload
    with open(filepath, "rb") as f:
        file_data = f.read()

    upload_req = urllib.request.Request(upload_url, data=file_data, method="PUT")
    # Required for S3 to process content type accurately if specified in options
    upload_req.add_header("Content-Type", mime_type)
    upload_res = urllib.request.urlopen(upload_req)
    etag = upload_res.headers.get("ETag")
    print(f"[2/5] Uploaded to S3 directly. ETag: {etag}")

    # 3. Complete
    complete_req = urllib.request.Request(
        f"{API_BASE}/uploads/{session_id}/complete",
        data=json.dumps({"parts": [{"partNumber": 1, "etag": etag}]}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(complete_req)
    print(f"[3/5] Multipart upload completed.")

    # 4. Poll
    blob_id = None
    for i in range(10):
        time.sleep(1)
        poll_res = urllib.request.urlopen(
            urllib.request.Request(f"{API_BASE}/uploads/{session_id}")
        )
        poll_data = json.loads(poll_res.read())
        status = poll_data.get("status")
        if status == "COMPLETED":
            blob_id = poll_data.get("blobId")
            break

    if not blob_id:
        print(f"❌ [4/5] FAILED: Hashing timeout.")
        sys.exit(1)

    print(f"[4/5] Async hashing finished. Blob ID: {blob_id}")

    # 5. Download & Verify
    dl_url_res = urllib.request.urlopen(
        urllib.request.Request(f"{API_BASE}/blobs/{blob_id}/download-url")
    )
    dl_url_data = json.loads(dl_url_res.read())
    download_url = dl_url_data["url"]
    print(f"[5/5] Download URL retrieved. Downloading...")

    dl_res = urllib.request.urlopen(download_url)
    downloaded_data = dl_res.read()

    if file_data == downloaded_data:
        print(
            f"✅ SUCCESS: {filename} uploaded and downloaded successfully. Bytes match perfectly!"
        )
    else:
        print(f"❌ FAILED: Data mismatch for {filename}.")
        sys.exit(1)


if __name__ == "__main__":
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    success_count = 0
    files = [
        f
        for f in os.listdir(fixtures_dir)
        if os.path.isfile(os.path.join(fixtures_dir, f))
    ]

    if not files:
        print("No test files found in tests/fixtures/")
        sys.exit(1)

    for f in files:
        path = os.path.join(fixtures_dir, f)
        run_test_for_file(path)
        success_count += 1

    print(f"\n🎉 All {success_count} files passed the E2E tests!")
