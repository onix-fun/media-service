package worker

import (
	"strings"
	"testing"
)

func TestContentV1ImageUsesWebPSaverMetadataStrip(t *testing.T) {
	profile, err := contentV1Profile("image-960")
	if err != nil {
		t.Fatalf("contentV1Profile returned error: %v", err)
	}
	joined := strings.Join(profile.Command, " ")
	if strings.Contains(joined, " --strip") {
		t.Fatalf("vipsthumbnail command must not contain unsupported --strip: %s", joined)
	}
	if !strings.Contains(joined, "[Q=85,strip]") {
		t.Fatalf("WebP saver must strip metadata: %s", joined)
	}
}

func TestContentV1VideoUsesEvenNoUpscaleDelivery(t *testing.T) {
	profile, err := contentV1Profile("video-1080")
	if err != nil {
		t.Fatalf("contentV1Profile returned error: %v", err)
	}
	joined := strings.Join(profile.Command, " ")
	for _, required := range []string{"force_divisible_by=2", "min(1920,iw)", "min(1080,ih)", "-pix_fmt yuv420p", "-map 0:a?"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("video delivery command is missing %q: %s", required, joined)
		}
	}
}
