package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service  Service            `yaml:"service"`
	Database Database           `yaml:"database"`
	S3       S3                 `yaml:"s3"`
	RabbitMQ RabbitMQ           `yaml:"rabbitmq"`
	Uploads  Uploads            `yaml:"uploads"`
	GC       GC                 `yaml:"gc"`
	Scanning Scanning           `yaml:"scanning"`
	Profiles map[string]Profile `yaml:"profiles"`
}
type Service struct {
	HTTPAddr string `yaml:"http_addr"`
	APIKey   string `yaml:"api_key"`
}
type Database struct {
	URL           string `yaml:"url"`
	AutoMigrate   bool   `yaml:"auto_migrate"`
	MigrationPath string `yaml:"migration_path"`
}
type S3 struct {
	Endpoint       string `yaml:"endpoint"`
	PublicEndpoint string `yaml:"public_endpoint"`
	AccessKey      string `yaml:"access_key"`
	SecretKey      string `yaml:"secret_key"`
	Bucket         string `yaml:"bucket"`
	Region         string `yaml:"region"`
	UseSSL         bool   `yaml:"use_ssl"`
	PublicUseSSL   bool   `yaml:"public_use_ssl"`
}
type RabbitMQ struct {
	URL            string `yaml:"url"`
	JobsExchange   string `yaml:"jobs_exchange"`
	JobsQueue      string `yaml:"jobs_queue"`
	JobsRoutingKey string `yaml:"jobs_routing_key"`
	EventsExchange string `yaml:"events_exchange"`
	DLQExchange    string `yaml:"dlq_exchange"`
	DLQQueue       string `yaml:"dlq_queue"`
	Prefetch       int    `yaml:"prefetch"`
	MaxRetries     int64  `yaml:"max_retries"`
}
type Uploads struct {
	UploadExpiry   time.Duration `yaml:"upload_expiry"`
	DownloadExpiry time.Duration `yaml:"download_expiry"`
	MaxSize        int64         `yaml:"max_size"`
	MaxParts       int           `yaml:"max_parts"`
}
type GC struct {
	Interval    time.Duration `yaml:"interval"`
	GracePeriod time.Duration `yaml:"grace_period"`
}
type Scanning struct {
	Enabled       bool   `yaml:"enabled"`
	ClamAVAddress string `yaml:"clamav_address"`
}
type Profile struct {
	Kind            string   `yaml:"kind"`
	Command         []string `yaml:"command"`
	MIME            []string `yaml:"mime"`
	OutputExtension string   `yaml:"output_extension"`
	OutputMIME      string   `yaml:"output_mime"`
	Automatic       bool     `yaml:"automatic"`
}

var env = regexp.MustCompile(`\$\{([A-Z0-9_]+)\}`)

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	raw := env.ReplaceAllStringFunc(string(data), func(s string) string { return os.Getenv(env.FindStringSubmatch(s)[1]) })
	var c Config
	if err = yaml.Unmarshal([]byte(raw), &c); err != nil {
		return Config{}, err
	}
	for _, target := range []*string{&c.Service.APIKey, &c.Database.URL, &c.S3.AccessKey, &c.S3.SecretKey, &c.RabbitMQ.URL} {
		if strings.HasPrefix(*target, "file:") {
			b, e := os.ReadFile(strings.TrimPrefix(*target, "file:"))
			if e != nil {
				return Config{}, e
			}
			*target = strings.TrimSpace(string(b))
		}
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Service.HTTPAddr == "" || c.Service.APIKey == "" || c.Database.URL == "" || c.S3.Endpoint == "" || c.S3.Bucket == "" || c.RabbitMQ.URL == "" {
		return errors.New("service, database, s3 and rabbitmq configuration is required")
	}
	if c.Uploads.UploadExpiry <= 0 || c.Uploads.DownloadExpiry <= 0 || c.Uploads.MaxSize <= 0 || c.Uploads.MaxParts <= 0 {
		return errors.New("upload limits and expiries must be positive")
	}
	if c.GC.Interval <= 0 || c.GC.GracePeriod <= 0 || c.RabbitMQ.Prefetch <= 0 || c.RabbitMQ.MaxRetries <= 0 {
		return errors.New("gc durations and rabbitmq prefetch must be positive")
	}
	for name, p := range c.Profiles {
		if name == "" || p.Kind == "" || len(p.Command) == 0 || p.OutputExtension == "" || p.OutputMIME == "" {
			return fmt.Errorf("profile %q is invalid", name)
		}
		if strings.ContainsAny(p.OutputExtension, `/\`) || !strings.HasPrefix(p.OutputExtension, ".") {
			return fmt.Errorf("profile %q output_extension must start with a dot and contain no path separators", name)
		}
	}
	return nil
}
func (u *Uploads) UnmarshalYAML(node *yaml.Node) error {
	type raw struct {
		UploadExpiry   string `yaml:"upload_expiry"`
		DownloadExpiry string `yaml:"download_expiry"`
		MaxSize        any    `yaml:"max_size"`
		MaxParts       int    `yaml:"max_parts"`
	}
	var r raw
	if err := node.Decode(&r); err != nil {
		return err
	}
	var err error
	u.UploadExpiry, err = time.ParseDuration(r.UploadExpiry)
	if err != nil {
		return err
	}
	u.DownloadExpiry, err = time.ParseDuration(r.DownloadExpiry)
	if err != nil {
		return err
	}
	u.MaxParts = r.MaxParts
	switch v := r.MaxSize.(type) {
	case int:
		u.MaxSize = int64(v)
	case string:
		u.MaxSize, err = strconv.ParseInt(v, 10, 64)
	}
	return err
}
func (g *GC) UnmarshalYAML(node *yaml.Node) error {
	var r struct {
		Interval    string `yaml:"interval"`
		GracePeriod string `yaml:"grace_period"`
	}
	if err := node.Decode(&r); err != nil {
		return err
	}
	var err error
	g.Interval, err = time.ParseDuration(r.Interval)
	if err != nil {
		return err
	}
	g.GracePeriod, err = time.ParseDuration(r.GracePeriod)
	return err
}
