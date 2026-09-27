package notifier

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

type HTTPAPI struct {
	service      *Service
	auth         *Authenticator
	maxBodyBytes int64
	mux          *http.ServeMux
}

func NewHTTPAPI(service *Service, auth *Authenticator, maxBodyBytes int64) http.Handler {
	api := &HTTPAPI{
		service:      service,
		auth:         auth,
		maxBodyBytes: maxBodyBytes,
		mux:          http.NewServeMux(),
	}
	api.mux.HandleFunc("POST /v1/notifications", api.createNotification)
	api.mux.HandleFunc("GET /v1/notifications/{id}", api.getNotification)
	api.mux.HandleFunc("GET /health/live", api.liveness)
	api.mux.HandleFunc("GET /health/ready", api.readiness)
	return api
}

func (a *HTTPAPI) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	requestID := validRequestID(request.Header.Get("X-Request-Id"))
	if requestID == "" {
		requestID = "req_" + ulid.Make().String()
	}
	response.Header().Set("X-Request-Id", requestID)
	request.Header.Set("X-Request-Id", requestID)
	a.mux.ServeHTTP(response, request)
}

func (a *HTTPAPI) createNotification(response http.ResponseWriter, request *http.Request) {
	requestID := request.Header.Get("X-Request-Id")
	principal, err := a.auth.Authenticate(request)
	if err != nil {
		writeAPIError(response, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required", requestID)
		return
	}
	if principal.Role != "producer" && principal.Role != "admin" {
		writeAPIError(response, http.StatusForbidden, "FORBIDDEN", "operation is not permitted", requestID)
		return
	}

	request.Body = http.MaxBytesReader(response, request.Body, a.maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	var input struct {
		IdempotencyKey string          `json:"idempotency_key"`
		Provider       string          `json:"provider"`
		EventType      string          `json:"event_type"`
		OccurredAt     *time.Time      `json:"occurred_at"`
		Payload        json.RawMessage `json:"payload"`
	}
	if decodeErr := decoder.Decode(&input); decodeErr != nil {
		a.writeDecodeError(response, requestID, decodeErr)
		return
	}
	var trailing any
	if decodeErr := decoder.Decode(&trailing); !errors.Is(decodeErr, io.EOF) {
		if decodeErr == nil {
			writeAPIError(response, http.StatusBadRequest, "INVALID_JSON", "multiple JSON documents are not allowed", requestID)
			return
		}
		a.writeDecodeError(response, requestID, decodeErr)
		return
	}

	result, err := a.service.Create(request.Context(), CreateInput{
		RequestID:      requestID,
		CallerID:       principal.CallerID,
		IdempotencyKey: input.IdempotencyKey,
		ProviderID:     input.Provider,
		EventType:      input.EventType,
		OccurredAt:     input.OccurredAt,
		Payload:        input.Payload,
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidRequest):
			writeAPIError(response, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), requestID)
		case errors.Is(err, ErrProviderNotFound):
			writeAPIError(response, http.StatusNotFound, "PROVIDER_NOT_FOUND", "provider not found or disabled", requestID)
		case errors.Is(err, ErrIdempotencyConflict):
			writeAPIError(response, http.StatusConflict, "IDEMPOTENCY_CONFLICT", ErrIdempotencyConflict.Error(), requestID)
		default:
			writeAPIError(response, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "notification could not be persisted", requestID)
		}
		return
	}

	writeJSON(response, http.StatusAccepted, struct {
		NotificationID string `json:"notification_id"`
		Status         Status `json:"status"`
		Duplicate      bool   `json:"duplicate"`
		StatusURL      string `json:"status_url"`
	}{
		NotificationID: result.Notification.ID,
		Status:         result.Notification.Status,
		Duplicate:      result.Duplicate,
		StatusURL:      "/v1/notifications/" + result.Notification.ID,
	})
}

func (a *HTTPAPI) getNotification(response http.ResponseWriter, request *http.Request) {
	requestID := request.Header.Get("X-Request-Id")
	principal, err := a.auth.Authenticate(request)
	if err != nil {
		writeAPIError(response, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required", requestID)
		return
	}

	notification, err := a.service.Get(
		request.Context(),
		request.PathValue("id"),
		principal.CallerID,
		principal.Role,
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeAPIError(response, http.StatusNotFound, "NOT_FOUND", "notification not found", requestID)
			return
		}
		writeAPIError(response, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "notification could not be read", requestID)
		return
	}

	writeJSON(response, http.StatusOK, notificationResponse{
		NotificationID:    notification.ID,
		Provider:          notification.ProviderID,
		EventType:         notification.EventType,
		Status:            notification.Status,
		Priority:          notification.Priority,
		DeliveryRound:     notification.DeliveryRound,
		AttemptCount:      notification.TotalAttemptCount,
		NextAttemptAt:     notification.NextAttemptAt,
		LastErrorCategory: notification.LastErrorCategory,
		LastHTTPStatus:    notification.LastHTTPStatus,
		LastErrorMessage:  notification.LastErrorMessage,
		CreatedAt:         notification.CreatedAt,
		UpdatedAt:         notification.UpdatedAt,
	})
}

func (a *HTTPAPI) liveness(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "live"})
}

func (a *HTTPAPI) readiness(response http.ResponseWriter, request *http.Request) {
	if err := a.service.store.Ping(request.Context()); err != nil {
		writeJSON(response, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *HTTPAPI) writeDecodeError(response http.ResponseWriter, requestID string, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeAPIError(response, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "request body exceeds configured limit", requestID)
		return
	}
	writeAPIError(response, http.StatusBadRequest, "INVALID_JSON", "request body must be one valid JSON object", requestID)
}

type notificationResponse struct {
	NotificationID    string        `json:"notification_id"`
	Provider          string        `json:"provider"`
	EventType         string        `json:"event_type"`
	Status            Status        `json:"status"`
	Priority          Priority      `json:"priority"`
	DeliveryRound     int           `json:"delivery_round"`
	AttemptCount      int           `json:"attempt_count"`
	NextAttemptAt     *time.Time    `json:"next_attempt_at,omitempty"`
	LastErrorCategory ErrorCategory `json:"last_error_category,omitempty"`
	LastHTTPStatus    *int          `json:"last_http_status,omitempty"`
	LastErrorMessage  string        `json:"last_error_message,omitempty"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
}

func writeAPIError(response http.ResponseWriter, status int, code, message, requestID string) {
	writeJSON(response, status, map[string]any{
		"error": map[string]string{
			"code":       code,
			"message":    message,
			"request_id": requestID,
		},
	})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func validRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return ""
		}
	}
	return value
}
