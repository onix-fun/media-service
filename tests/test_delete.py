import urllib.request
import json
import time

def test_delete():
    # 1. Init & Upload a test file
    print("Uploading a file to test DELETE...")
    req = urllib.request.Request("http://localhost:8080/uploads/init", data=b'{"mimeType":"text/plain", "expectedSize":12, "partsCount":1}', headers={"Content-Type": "application/json"})
    res = urllib.request.urlopen(req)
    data = json.loads(res.read())
    session_id = data["sessionId"]
    url = data["parts"]["1"]
    
    req2 = urllib.request.Request(url, data=b"Delete Test!", method="PUT")
    res2 = urllib.request.urlopen(req2)
    etag = res2.headers.get("ETag")
    
    complete_data = json.dumps({"parts": [{"partNumber": 1, "etag": etag}]}).encode("utf-8")
    req3 = urllib.request.Request(f"http://localhost:8080/uploads/{session_id}/complete", data=complete_data, headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req3)
    
    blob_id = None
    for i in range(5):
        time.sleep(1)
        res4 = urllib.request.urlopen(urllib.request.Request(f"http://localhost:8080/uploads/{session_id}"))
        final_data = json.loads(res4.read())
        if final_data.get("status") == "COMPLETED":
            blob_id = final_data.get("blobId")
            break

    print(f"File uploaded successfully. Blob ID: {blob_id}")
    
    # 2. Call DELETE endpoint
    print(f"Calling DELETE /blobs/{blob_id} ...")
    del_req = urllib.request.Request(f"http://localhost:8080/blobs/{blob_id}", method="DELETE")
    del_res = urllib.request.urlopen(del_req)
    print(f"DELETE Response Status: {del_res.status}")
    
    # 3. Verify it's gone
    print("Verifying it is deleted...")
    try:
        urllib.request.urlopen(urllib.request.Request(f"http://localhost:8080/blobs/{blob_id}/download-url"))
        print("FAILED: File still exists!")
    except urllib.error.HTTPError as e:
        print(f"SUCCESS: Verification returned HTTP {e.code} (Expected 500/404)")

test_delete()
