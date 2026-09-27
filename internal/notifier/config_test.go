package notifier

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolvePriorityUsesServerMapping(t *testing.T) {
	cfg := PriorityConfig{
		Default: "NORMAL",
		Events:  map[string]string{"payment.succeeded": "HIGH"},
	}

	got, err := ResolvePriority(cfg, "payment.succeeded")
	if err != nil {
		t.Fatalf("ResolvePriority() error = %v", err)
	}
	if got != PriorityHigh {
		t.Fatalf("ResolvePriority() = %v, want %v", got, PriorityHigh)
	}
}

func TestResolvePriorityUsesConfiguredDefault(t *testing.T) {
	cfg := PriorityConfig{Default: "LOW"}

	got, err := ResolvePriority(cfg, "unmapped.event")
	if err != nil {
		t.Fatalf("ResolvePriority() error = %v", err)
	}
	if got != PriorityLow {
		t.Fatalf("ResolvePriority() = %v, want %v", got, PriorityLow)
	}
}

func TestResolvePriorityRejectsInvalidMapping(t *testing.T) {
	cfg := PriorityConfig{
		Default: "NORMAL",
		Events:  map[string]string{"payment.succeeded": "URGENT"},
	}

	if _, err := ResolvePriority(cfg, "payment.succeeded"); err == nil {
		t.Fatal("ResolvePriority() error = nil, want invalid priority error")
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("environment: development\nunknown: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() error = nil, want unknown field error")
	}
}

func TestProductionRejectsUnsafeNetworkPolicy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name: "disabled authentication",
			mutate: func(cfg *Config) {
				cfg.Auth.Mode = "disabled"
			},
			want: "authentication",
		},
		{
			name: "insecure HTTP",
			mutate: func(cfg *Config) {
				cfg.NetworkPolicy.AllowInsecureHTTP = true
			},
			want: "insecure HTTP",
		},
		{
			name: "private IP access",
			mutate: func(cfg *Config) {
				cfg.NetworkPolicy.AllowPrivateIP = true
			},
			want: "private IP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Environment: "production",
				Auth:        AuthConfig{Mode: "static_token"},
			}
			tt.mutate(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want unsafe production config error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestResolveSecretsReadsEnvironment(t *testing.T) {
	t.Setenv("ORDER_SERVICE_NOTIFY_TOKEN", "producer-secret")
	t.Setenv("CRM_BEARER_TOKEN", "provider-secret")
	cfg := Config{
		Auth: AuthConfig{
			Principals: []AuthPrincipalConfig{
				{CallerID: "order-service", Role: "producer", TokenEnv: "ORDER_SERVICE_NOTIFY_TOKEN"},
			},
		},
		Providers: map[string]ProviderConfig{
			"crm": {
				Auth: ProviderAuthConfig{Type: "bearer", TokenEnv: "CRM_BEARER_TOKEN"},
			},
		},
	}

	if err := cfg.ResolveSecrets(); err != nil {
		t.Fatalf("ResolveSecrets() error = %v", err)
	}
	if cfg.Auth.Principals[0].token != "producer-secret" {
		t.Fatal("producer token was not resolved")
	}
	if cfg.Providers["crm"].Auth.secret != "provider-secret" {
		t.Fatal("provider token was not resolved")
	}
}

func TestResolveSecretsRejectsMissingEnvironmentVariable(t *testing.T) {
	cfg := Config{
		Auth: AuthConfig{
			Principals: []AuthPrincipalConfig{
				{CallerID: "order-service", Role: "producer", TokenEnv: "MISSING_NOTIFY_TOKEN"},
			},
		},
	}

	if err := cfg.ResolveSecrets(); err == nil {
		t.Fatal("ResolveSecrets() error = nil, want missing environment variable error")
	}
}

func TestDisabledAuthenticationRequiresLoopbackListener(t *testing.T) {
	cfg := validConfigFixture()
	cfg.Auth.Mode = "disabled"
	cfg.Server.Address = "0.0.0.0:8080"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Validate() error = %v, want loopback restriction", err)
	}
}

func validConfigFixture() Config {
	return Config{
		Environment: "development",
		Server: ServerConfig{
			Address:           "127.0.0.1:8080",
			MaxBodyBytes:      1024,
			ShutdownGrace:     time.Second,
			ReadHeaderTimeout: time.Second,
			ReadTimeout:       time.Second,
			WriteTimeout:      time.Second,
			IdleTimeout:       time.Second,
		},
		Database: DatabaseConfig{Path: "test.db", BusyTimeout: time.Second},
		Scheduler: SchedulerConfig{
			PollInterval:        time.Millisecond,
			ProviderScanLimit:   1,
			RecoveryBatchSize:   1,
			LeaseDuration:       time.Second,
			FinalizeRetryBudget: time.Millisecond,
		},
		Workers:  WorkersConfig{Concurrency: 1},
		Priority: PriorityConfig{Default: "NORMAL", AgingInterval: time.Second},
		Retry: RetryConfig{
			InitialDelay:       time.Second,
			MaxDelay:           time.Minute,
			JitterRatio:        0.2,
			DefaultMaxAttempts: 3,
		},
		Auth: AuthConfig{Mode: "static_token"},
	}
}
