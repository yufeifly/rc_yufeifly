package notifier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const requestHashVersion = 1

type Clock interface {
	Now() time.Time
}

type CreateInput struct {
	RequestID      string
	CallerID       string
	IdempotencyKey string
	ProviderID     string
	EventType      string
	OccurredAt     *time.Time
	Payload        json.RawMessage
}

type Service struct {
	store *Store
	cfg   Config
	clock Clock
	wake  func()
}

func NewService(store *Store, cfg Config, clock Clock) *Service {
	if clock == nil {
		clock = systemClock{}
	}
	return &Service{store: store, cfg: cfg, clock: clock}
}

func (s *Service) SetWake(wake func()) {
	s.wake = wake
}

func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	if err := validateCreateInput(in); err != nil {
		return CreateResult{}, err
	}

	provider, ok := s.cfg.Providers[in.ProviderID]
	if !ok || !provider.Enabled {
		return CreateResult{}, ErrProviderNotFound
	}
	priority, err := ResolvePriority(s.cfg.Priority, in.EventType)
	if err != nil {
		return CreateResult{}, fmt.Errorf("resolve priority: %w", err)
	}
	canonicalPayload, err := canonicalJSON(in.Payload)
	if err != nil {
		return CreateResult{}, fmt.Errorf("%w: payload must be one valid JSON object: %v", ErrInvalidRequest, err)
	}
	var payloadObject map[string]json.RawMessage
	if err := json.Unmarshal(canonicalPayload, &payloadObject); err != nil || payloadObject == nil {
		return CreateResult{}, fmt.Errorf("%w: payload must be a JSON object", ErrInvalidRequest)
	}

	now := s.clock.Now().UTC()
	occurredAt := now
	occurredAtHash := "server-assigned"
	if in.OccurredAt != nil {
		occurredAt = in.OccurredAt.UTC()
		occurredAtHash = occurredAt.Format(time.RFC3339Nano)
	}

	maxAttempts := provider.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = s.cfg.Retry.DefaultMaxAttempts
	}
	if maxAttempts <= 0 {
		return CreateResult{}, errors.New("provider maximum attempts must be positive")
	}

	result, err := s.store.CreateOrGet(ctx, CreateNotification{
		ID:                 "ntf_" + ulid.Make().String(),
		RequestID:          in.RequestID,
		CallerID:           in.CallerID,
		IdempotencyKey:     in.IdempotencyKey,
		RequestHash:        fullRequestHash(in.ProviderID, in.EventType, occurredAtHash, canonicalPayload),
		RequestHashVersion: requestHashVersion,
		ProviderID:         in.ProviderID,
		EventType:          in.EventType,
		Priority:           priority,
		OccurredAt:         occurredAt,
		Payload:            canonicalPayload,
		MaxAttempts:        maxAttempts,
		CreatedAt:          now,
	})
	if err == nil && !result.Duplicate && s.wake != nil {
		s.wake()
	}
	return result, err
}

func (s *Service) Get(ctx context.Context, id, callerID, role string) (Notification, error) {
	notification, err := s.store.Get(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	if role != "admin" && notification.CallerID != callerID {
		return Notification{}, ErrNotFound
	}
	return notification, nil
}

func (s *Service) Replay(ctx context.Context, id, operatorID, reason string) (Notification, error) {
	id = strings.TrimSpace(id)
	operatorID = strings.TrimSpace(operatorID)
	reason = strings.TrimSpace(reason)
	if id == "" || operatorID == "" || reason == "" {
		return Notification{}, fmt.Errorf("%w: notification ID, operator ID and reason are required", ErrInvalidRequest)
	}
	if len(reason) > 500 {
		return Notification{}, fmt.Errorf("%w: replay reason exceeds 500 bytes", ErrInvalidRequest)
	}
	notification, err := s.store.ReplayDead(ctx, ReplayRequest{
		ID:         id,
		AuditID:    "rpl_" + ulid.Make().String(),
		OperatorID: operatorID,
		Reason:     reason,
		Now:        s.clock.Now().UTC(),
	})
	if err == nil && s.wake != nil {
		s.wake()
	}
	return notification, err
}

func validateCreateInput(in CreateInput) error {
	required := map[string]string{
		"request_id":      in.RequestID,
		"caller_id":       in.CallerID,
		"idempotency_key": in.IdempotencyKey,
		"provider":        in.ProviderID,
		"event_type":      in.EventType,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidRequest, name)
		}
	}
	return nil
}

func hashRequest(payload json.RawMessage) string {
	canonical, err := canonicalJSON(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func fullRequestHash(providerID, eventType, occurredAt string, payload json.RawMessage) string {
	document := struct {
		Version       int             `json:"version"`
		ProviderID    string          `json:"provider_id"`
		EventType     string          `json:"event_type"`
		OccurredAt    string          `json:"occurred_at"`
		CanonicalJSON json.RawMessage `json:"payload"`
	}{
		Version:       requestHashVersion,
		ProviderID:    providerID,
		EventType:     eventType,
		OccurredAt:    occurredAt,
		CanonicalJSON: payload,
	}
	encoded, _ := json.Marshal(document)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(input json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON documents are not allowed")
		}
		return nil, err
	}
	return json.Marshal(value)
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
