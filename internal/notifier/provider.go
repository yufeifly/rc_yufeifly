package notifier

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultMaxResponseBytes int64 = 16 << 10

var reservedProviderHeaders = map[string]struct{}{
	"authorization":      {},
	"content-length":     {},
	"content-type":       {},
	"host":               {},
	"user-agent":         {},
	"x-delivery-attempt": {},
	"x-delivery-round":   {},
	"x-notification-id":  {},
	"x-request-id":       {},
}

type ProviderRegistry struct {
	providers map[string]*Provider
}

type Provider struct {
	ID        string
	Version   string
	Config    ProviderConfig
	Client    *http.Client
	semaphore chan struct{}
}

type ipResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

func NewProviderRegistry(cfg Config) (*ProviderRegistry, error) {
	registry := &ProviderRegistry{providers: make(map[string]*Provider, len(cfg.Providers))}
	for id, providerConfig := range cfg.Providers {
		if !providerConfig.Enabled {
			continue
		}
		if err := validateRuntimeProvider(id, providerConfig, cfg.NetworkPolicy); err != nil {
			return nil, err
		}

		configCopy := providerConfig
		configCopy.Headers = cloneHeaders(providerConfig.Headers)
		connectTimeout := configCopy.Timeout.Connect
		if connectTimeout <= 0 {
			connectTimeout = 3 * time.Second
		}
		requestTimeout := configCopy.Timeout.Request
		if requestTimeout <= 0 {
			requestTimeout = 10 * time.Second
		}
		transport := &http.Transport{
			Proxy:                 nil,
			DialContext:           safeDialContext(&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}, net.DefaultResolver, cfg.NetworkPolicy.AllowPrivateIP),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          configCopy.MaxConcurrency * 2,
			MaxIdleConnsPerHost:   configCopy.MaxConcurrency,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   connectTimeout,
			ResponseHeaderTimeout: requestTimeout,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		}
		registry.providers[id] = &Provider{
			ID:      id,
			Version: configCopy.Version,
			Config:  configCopy,
			Client: &http.Client{
				Transport: transport,
				Timeout:   requestTimeout,
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return http.ErrUseLastResponse
				},
			},
			semaphore: make(chan struct{}, configCopy.MaxConcurrency),
		}
	}
	return registry, nil
}

func (r *ProviderRegistry) Get(id string) (*Provider, bool) {
	provider, ok := r.providers[id]
	return provider, ok
}

func (p *Provider) TryAcquire() bool {
	select {
	case p.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *Provider) Release() {
	select {
	case <-p.semaphore:
	default:
		panic("notifier: release provider permit without acquire")
	}
}

func (p *Provider) Deliver(ctx context.Context, task ClaimedNotification) DeliveryOutcome {
	request, err := p.buildRequest(ctx, task)
	if err != nil {
		return DeliveryOutcome{
			Category:    ErrorRequestBuild,
			SafeMessage: "could not build provider request",
		}
	}

	var wroteRequest bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) {
			wroteRequest = true
		},
	}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	response, err := p.Client.Do(request)
	if err != nil {
		return classifyCallError(ctx, err, wroteRequest)
	}
	defer response.Body.Close()

	maxBytes := p.Config.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxBytes+1))

	if p.accepts(response.StatusCode) {
		return DeliveryOutcome{
			Success:    true,
			Category:   ErrorNone,
			HTTPStatus: intPointer(response.StatusCode),
		}
	}
	outcome := classifyResponse(response.StatusCode, response.Header.Get("Retry-After"), time.Now())
	if outcome.Success {
		outcome.Success = false
		outcome.Category = ErrorUpstream4XX
		outcome.SafeMessage = "upstream returned an unaccepted status"
	}
	return outcome
}

func (p *Provider) buildRequest(ctx context.Context, task ClaimedNotification) (*http.Request, error) {
	body, err := p.requestBody(task)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, p.Config.Method, p.Config.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	for name, value := range p.Config.Headers {
		if _, reserved := reservedProviderHeaders[strings.ToLower(name)]; reserved {
			continue
		}
		request.Header.Set(name, value)
	}
	switch p.Config.Auth.Type {
	case "", "none":
	case "bearer":
		request.Header.Set("Authorization", "Bearer "+p.Config.Auth.secret)
	case "api_key_header":
		request.Header.Set(p.Config.Auth.Header, p.Config.Auth.secret)
	default:
		return nil, fmt.Errorf("unsupported authentication type %q", p.Config.Auth.Type)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "reliable-http-notifier/1")
	request.Header.Set("X-Notification-Id", task.ID)
	request.Header.Set("X-Delivery-Attempt", strconv.Itoa(task.TotalAttemptCount))
	request.Header.Set("X-Delivery-Round", strconv.Itoa(task.DeliveryRound))
	request.Header.Set("X-Request-Id", task.RequestID)
	if p.Config.IdempotencyHeader != "" {
		request.Header.Set(p.Config.IdempotencyHeader, task.ID)
	}
	return request, nil
}

func (p *Provider) requestBody(task ClaimedNotification) ([]byte, error) {
	switch p.Config.BodyMode {
	case "passthrough":
		if !json.Valid(task.Payload) {
			return nil, errors.New("invalid persisted payload")
		}
		return append([]byte(nil), task.Payload...), nil
	case "envelope":
		return json.Marshal(struct {
			NotificationID string          `json:"notification_id"`
			EventType      string          `json:"event_type"`
			OccurredAt     time.Time       `json:"occurred_at"`
			Payload        json.RawMessage `json:"payload"`
		}{
			NotificationID: task.ID,
			EventType:      task.EventType,
			OccurredAt:     task.OccurredAt,
			Payload:        task.Payload,
		})
	default:
		return nil, fmt.Errorf("unsupported body mode %q", p.Config.BodyMode)
	}
}

func (p *Provider) accepts(status int) bool {
	for _, accepted := range p.Config.SuccessStatusCodes {
		if accepted == status {
			return true
		}
	}
	return false
}

func validateRuntimeProvider(id string, cfg ProviderConfig, policy NetworkPolicyConfig) error {
	parsed, err := url.Parse(cfg.URL)
	if err != nil || parsed.Host == "" || parsed.Scheme == "" {
		return fmt.Errorf("provider %q has invalid URL", id)
	}
	if parsed.User != nil {
		return fmt.Errorf("provider %q URL cannot contain credentials", id)
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && policy.AllowInsecureHTTP) {
		return fmt.Errorf("provider %q URL scheme is not allowed", id)
	}
	if literal := net.ParseIP(parsed.Hostname()); literal != nil && !policy.AllowPrivateIP && !isPublicIP(literal) {
		return fmt.Errorf("provider %q: %w", id, ErrUnsafeDestination)
	}
	if cfg.Method == "" {
		return fmt.Errorf("provider %q method is required", id)
	}
	if _, err := http.NewRequest(cfg.Method, cfg.URL, nil); err != nil {
		return fmt.Errorf("provider %q has invalid HTTP request configuration: %w", id, err)
	}
	if cfg.BodyMode != "envelope" && cfg.BodyMode != "passthrough" {
		return fmt.Errorf("provider %q has invalid body mode", id)
	}
	if len(cfg.SuccessStatusCodes) == 0 {
		return fmt.Errorf("provider %q must configure success status codes", id)
	}
	for _, status := range cfg.SuccessStatusCodes {
		if status < 200 || status > 299 {
			return fmt.Errorf("provider %q has invalid success status %d", id, status)
		}
	}
	for name, value := range cfg.Headers {
		if !validHeaderName(name) || !validHeaderValue(value) {
			return fmt.Errorf("provider %q has an invalid static header", id)
		}
	}
	if cfg.IdempotencyHeader != "" {
		if !validHeaderName(cfg.IdempotencyHeader) {
			return fmt.Errorf("provider %q has an invalid idempotency header", id)
		}
		if _, reserved := reservedProviderHeaders[strings.ToLower(cfg.IdempotencyHeader)]; reserved {
			return fmt.Errorf("provider %q idempotency header conflicts with a reserved header", id)
		}
	}
	if cfg.MaxConcurrency <= 0 {
		return fmt.Errorf("provider %q maximum concurrency must be positive", id)
	}
	switch cfg.Auth.Type {
	case "", "none":
	case "bearer":
		if cfg.Auth.secret == "" {
			return fmt.Errorf("provider %q bearer credential is empty", id)
		}
	case "api_key_header":
		if cfg.Auth.Header == "" || cfg.Auth.secret == "" {
			return fmt.Errorf("provider %q API key configuration is incomplete", id)
		}
		if !validHeaderName(cfg.Auth.Header) {
			return fmt.Errorf("provider %q has an invalid API key header", id)
		}
		if _, reserved := reservedProviderHeaders[strings.ToLower(cfg.Auth.Header)]; reserved {
			return fmt.Errorf("provider %q API key header conflicts with a reserved header", id)
		}
	default:
		return fmt.Errorf("provider %q has invalid authentication type", id)
	}
	return nil
}

func safeDialContext(dialer *net.Dialer, resolver ipResolver, allowPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split destination address: %w", err)
		}
		addresses, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			return nil, &net.DNSError{Name: host, Err: "no addresses"}
		}

		var lastError error
		for _, address := range addresses {
			if !allowPrivate && !isPublicIP(address.IP) {
				lastError = ErrUnsafeDestination
				continue
			}
			target := net.JoinHostPort(address.IP.String(), port)
			connection, dialError := dialer.DialContext(ctx, network, target)
			if dialError == nil {
				return connection, nil
			}
			lastError = dialError
		}
		if lastError == nil {
			lastError = ErrUnsafeDestination
		}
		return nil, lastError
	}
}

func isPublicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, network := range blockedPublicLikeNetworks {
		if network.Contains(ip) {
			return false
		}
	}
	return true
}

var blockedPublicLikeNetworks = mustParseNetworks(
	"100.64.0.0/10",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"2001:db8::/32",
)

func mustParseNetworks(values ...string) []*net.IPNet {
	networks := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			panic(err)
		}
		networks = append(networks, network)
	}
	return networks
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for name, value := range headers {
		cloned[name] = value
	}
	return cloned
}

func validHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		character := value[i]
		if !((character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character))) {
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	return !strings.ContainsAny(value, "\r\n\x00")
}
