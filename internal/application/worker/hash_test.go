package worker

import "testing"

func TestDetectMediaMIMERecognizesBrowserRecorderWebM(t *testing.T) {
	prefix := append([]byte{0x1a, 0x45, 0xdf, 0xa3, 0x9f, 0x42, 0x82}, []byte("webm")...)
	if got := detectMediaMIME(prefix); got != "video/webm" {
		t.Fatalf("detectMediaMIME() = %q, want video/webm", got)
	}
}
