package jobs

import "testing"

func TestAssetJobKeyChangesForRetryGeneration(t *testing.T) {
	first, err := jobKey(Job{Type: "asset", AssetID: "11111111-1111-1111-1111-111111111111", Generation: 1})
	if err != nil {
		t.Fatalf("first key error: %v", err)
	}
	second, err := jobKey(Job{Type: "asset", AssetID: "11111111-1111-1111-1111-111111111111", Generation: 2})
	if err != nil {
		t.Fatalf("second key error: %v", err)
	}
	if first == second {
		t.Fatalf("retry must use a distinct durable job key: %q", first)
	}
}
