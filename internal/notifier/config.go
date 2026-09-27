package notifier

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Environment   string                    `yaml:"environment"`
	Server        ServerConfig              `yaml:"server"`
	Database      DatabaseConfig            `yaml:"database"`
	Scheduler     SchedulerConfig           `yaml:"scheduler"`
	Workers       WorkersConfig             `yaml:"workers"`
	Priority      PriorityConfig            `yaml:"priority"`
	Retry         RetryConfig               `yaml:"retry"`
	Auth          AuthConfig                `yaml:"auth"`
	NetworkPolicy NetworkPolicyConfig       `yaml:"network_policy"`
	Providers     map[string]ProviderConfig `yaml:"providers"`
}

type ServerConfig struct {
	Address           string        `yaml:"address"`
	MaxBodyBytes      int64         `yaml:"max_body_bytes"`
	ShutdownGrace     time.Duration `yaml:"shutdown_grace"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
}

type DatabaseConfig struct {
	Path        string        `yaml:"path"`
	BusyTimeout time.Duration `yaml:"busy_timeout"`
}

type SchedulerConfig struct {
	PollInterval        time.Duration `yaml:"poll_interval"`
	ProviderScanLimit   int           `yaml:"provider_scan_limit"`
	RecoveryBatchSize   int           `yaml:"recovery_batch_size"`
	LeaseDuration       time.Duration `yaml:"lease_duration"`
	FinalizeRetryBudget time.Duration `yaml:"finalize_retry_budget"`
}

type WorkersConfig struct {
	Concurrency int `yaml:"concurrency"`
}

type PriorityConfig struct {
	Default       string            `yaml:"default"`
	AgingInterval time.Duration     `yaml:"aging_interval"`
	Events        map[string]string `yaml:"events"`
}

type RetryConfig struct {
	InitialDelay       time.Duration `yaml:"initial_delay"`
	MaxDelay           time.Duration `yaml:"max_delay"`
	JitterRatio        float64       `yaml:"jitter_ratio"`
	DefaultMaxAttempts int           `yaml:"default_max_attempts"`
}

type AuthConfig struct {
	Mode       string                `yaml:"mode"`
	Principals []AuthPrincipalConfig `yaml:"principals"`
}

type AuthPrincipalConfig struct {
	CallerID string `yaml:"caller_id"`
	Role     string `yaml:"role"`
	TokenEnv string `yaml:"token_env"`
	token    string
}

type NetworkPolicyConfig struct {
	AllowInsecureHTTP bool `yaml:"allow_insecure_http"`
	AllowPrivateIP    bool `yaml:"allow_private_ip"`
}

type ProviderConfig struct {
	Version            string                `yaml:"version"`
	Enabled            bool                  `yaml:"enabled"`
	Method             string                `yaml:"method"`
	URL                string                `yaml:"url"`
	BodyMode           string                `yaml:"body_mode"`
	Headers            map[string]string     `yaml:"headers"`
	SuccessStatusCodes []int                 `yaml:"success_status_codes"`
	MaxResponseBytes   int64                 `yaml:"max_response_bytes"`
	IdempotencyHeader  string                `yaml:"idempotency_header"`
	Auth               ProviderAuthConfig    `yaml:"auth"`
	Timeout            ProviderTimeoutConfig `yaml:"timeout"`
	Retry              ProviderRetryConfig   `yaml:"retry"`
	MaxConcurrency     int                   `yaml:"max_concurrency"`
}

type ProviderAuthConfig struct {
	Type     string `yaml:"type"`
	TokenEnv string `yaml:"token_env"`
	Header   string `yaml:"header"`
	ValueEnv string `yaml:"value_env"`
	secret   string
}

type ProviderTimeoutConfig struct {
	Connect time.Duration `yaml:"connect"`
	Request time.Duration `yaml:"request"`
}

type ProviderRetryConfig struct {
	MaxAttempts int `yaml:"max_attempts"`
}

func ResolvePriority(cfg PriorityConfig, eventType string) (Priority, error) {
	value := cfg.Default
	if mapped, ok := cfg.Events[eventType]; ok {
		value = mapped
	}

	switch strings.ToUpper(value) {
	case "LOW":
		return PriorityLow, nil
	case "NORMAL":
		return PriorityNormal, nil
	case "HIGH":
		return PriorityHigh, nil
	default:
		return 0, fmt.Errorf("invalid priority %q", value)
	}
}

func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode config: multiple YAML documents are not allowed")
		}
		return Config{}, fmt.Errorf("decode trailing config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	switch c.Environment {
	case "", "development", "production":
	default:
		return fmt.Errorf("invalid environment %q", c.Environment)
	}

	switch c.Auth.Mode {
	case "", "disabled", "static_token":
	default:
		return fmt.Errorf("invalid authentication mode %q", c.Auth.Mode)
	}

	if c.Environment == "production" {
		if c.Auth.Mode != "static_token" {
			return errors.New("production requires static token authentication")
		}
		if c.NetworkPolicy.AllowInsecureHTTP {
			return errors.New("production cannot allow insecure HTTP")
		}
		if c.NetworkPolicy.AllowPrivateIP {
			return errors.New("production cannot allow private IP access")
		}
	}

	if c.Priority.Default != "" {
		if _, err := ResolvePriority(c.Priority, ""); err != nil {
			return fmt.Errorf("default priority: %w", err)
		}
	}
	for eventType, priority := range c.Priority.Events {
		if strings.TrimSpace(eventType) == "" {
			return errors.New("priority event type cannot be empty")
		}
		if _, err := ResolvePriority(PriorityConfig{Default: priority}, ""); err != nil {
			return fmt.Errorf("priority for event %q: %w", eventType, err)
		}
	}

	for id, provider := range c.Providers {
		if strings.TrimSpace(id) == "" {
			return errors.New("provider ID cannot be empty")
		}
		if provider.URL == "" {
			continue
		}
		parsed, err := url.ParseRequestURI(provider.URL)
		if err != nil || parsed.Host == "" {
			return fmt.Errorf("provider %q has invalid URL", id)
		}
	}
	return nil
}

func (c *Config) ResolveSecrets() error {
	for i := range c.Auth.Principals {
		principal := &c.Auth.Principals[i]
		secret, err := requiredEnvironmentVariable(principal.TokenEnv)
		if err != nil {
			return fmt.Errorf("principal %q token: %w", principal.CallerID, err)
		}
		principal.token = secret
	}

	for id, provider := range c.Providers {
		envName := provider.Auth.TokenEnv
		if envName == "" {
			envName = provider.Auth.ValueEnv
		}
		if envName == "" {
			continue
		}
		secret, err := requiredEnvironmentVariable(envName)
		if err != nil {
			return fmt.Errorf("provider %q credential: %w", id, err)
		}
		provider.Auth.secret = secret
		c.Providers[id] = provider
	}
	return nil
}

func requiredEnvironmentVariable(name string) (string, error) {
	if name == "" {
		return "", errors.New("environment variable name is empty")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("environment variable %q is not set", name)
	}
	return value, nil
}
