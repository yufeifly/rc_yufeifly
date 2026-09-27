package notifier

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

var (
	ErrUnauthorized = errors.New("authentication required")
	ErrForbidden    = errors.New("operation is not permitted")
)

type Principal struct {
	CallerID string
	Role     string
}

type Authenticator struct {
	mode       string
	principals []authPrincipal
}

type authPrincipal struct {
	Principal
	token []byte
}

func NewAuthenticator(cfg AuthConfig) (*Authenticator, error) {
	authenticator := &Authenticator{mode: cfg.Mode}
	if cfg.Mode == "disabled" {
		return authenticator, nil
	}
	if cfg.Mode != "static_token" {
		return nil, errors.New("unsupported authentication mode")
	}

	seenTokens := make(map[string]struct{}, len(cfg.Principals))
	for _, configured := range cfg.Principals {
		if configured.CallerID == "" || configured.token == "" {
			return nil, errors.New("authentication principal requires caller ID and resolved token")
		}
		if configured.Role != "producer" && configured.Role != "admin" {
			return nil, errors.New("authentication principal has unsupported role")
		}
		if _, exists := seenTokens[configured.token]; exists {
			return nil, errors.New("authentication token maps to multiple principals")
		}
		seenTokens[configured.token] = struct{}{}
		authenticator.principals = append(authenticator.principals, authPrincipal{
			Principal: Principal{CallerID: configured.CallerID, Role: configured.Role},
			token:     []byte(configured.token),
		})
	}
	return authenticator, nil
}

func (a *Authenticator) Authenticate(request *http.Request) (Principal, error) {
	if a.mode == "disabled" {
		callerID := strings.TrimSpace(request.Header.Get("X-Caller-Id"))
		if callerID == "" {
			callerID = "development"
		}
		return Principal{CallerID: callerID, Role: "producer"}, nil
	}

	const prefix = "Bearer "
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, prefix) {
		return Principal{}, ErrUnauthorized
	}
	presented := []byte(strings.TrimSpace(strings.TrimPrefix(authorization, prefix)))

	var matched *Principal
	for i := range a.principals {
		candidate := &a.principals[i]
		if subtle.ConstantTimeCompare(candidate.token, presented) == 1 {
			principal := candidate.Principal
			matched = &principal
		}
	}
	if matched == nil {
		return Principal{}, ErrUnauthorized
	}
	return *matched, nil
}
