package config

import "testing"

func TestValidateAllowsContentPipelineWithoutRawCommands(t *testing.T) {
	config := Config{
		Service:  Service{HTTPAddr: ":8080", GRPCAddr: ":9093", APIKey: "test"},
		Database: Database{URL: "postgres://test"},
		S3:       S3{Endpoint: "minio:9000", Bucket: "media-blobs"},
		Jobs:     Jobs{PollInterval: 1, LeaseDuration: 1, BatchSize: 1, MaxRetries: 1},
		Uploads:  Uploads{UploadExpiry: 1, DownloadExpiry: 1, MaxSize: 1, MaxParts: 1},
		GC:       GC{Interval: 1, GracePeriod: 1},
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
