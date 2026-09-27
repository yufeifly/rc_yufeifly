package notifier

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type Scheduler struct {
	cfg       Config
	store     *Store
	providers *ProviderRegistry
	log       *slog.Logger
	clock     Clock
	workerID  string

	globalPermits chan struct{}
	wake          chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc
	cursor int
	wg     sync.WaitGroup
}

func NewScheduler(cfg Config, store *Store, providers *ProviderRegistry, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	concurrency := cfg.Workers.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Scheduler{
		cfg:           cfg,
		store:         store,
		providers:     providers,
		log:           log,
		clock:         systemClock{},
		workerID:      "worker_" + ulid.Make().String(),
		globalPermits: make(chan struct{}, concurrency),
		wake:          make(chan struct{}, 1),
	}
}

func (s *Scheduler) Start(parent context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.wg.Add(1)
	s.mu.Unlock()

	go s.run(ctx)
}

func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()
	pollInterval := s.cfg.Scheduler.PollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-timer.C:
		}

		s.dispatch(ctx)
		timer.Reset(pollInterval)
	}
}

func (s *Scheduler) dispatch(ctx context.Context) {
	now := s.clock.Now().UTC()
	recoveryLimit := s.cfg.Scheduler.RecoveryBatchSize
	if recoveryLimit <= 0 {
		recoveryLimit = 100
	}
	if _, err := s.store.RecoverExpired(ctx, now, recoveryLimit); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Error("recover expired leases", "error", err)
		}
		return
	}
	providers, err := s.store.ListDueProviders(ctx, now, s.cfg.Priority.AgingInterval)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Error("list due providers", "error", err)
		}
		return
	}
	if len(providers) == 0 {
		return
	}

	start := s.cursor % len(providers)
	for offset := range providers {
		index := (start + offset) % len(providers)
		due := providers[index]
		if !tryAcquire(s.globalPermits) {
			break
		}

		provider, configured := s.providers.Get(due.ProviderID)
		if configured && !provider.TryAcquire() {
			release(s.globalPermits)
			continue
		}
		providerVersion := ""
		if configured {
			providerVersion = provider.Version
		}
		claimed, claimErr := s.store.ClaimAndStart(ctx, ClaimRequest{
			ProviderID:      due.ProviderID,
			ProviderVersion: providerVersion,
			WorkerID:        s.workerID,
			Now:             now,
			LeaseDuration:   s.cfg.Scheduler.LeaseDuration,
			AgingInterval:   s.cfg.Priority.AgingInterval,
		})
		if claimErr != nil {
			if configured {
				provider.Release()
			}
			release(s.globalPermits)
			if !errors.Is(claimErr, ErrNotFound) && !errors.Is(claimErr, context.Canceled) {
				s.log.Error("claim notification", "provider_id", due.ProviderID, "error", claimErr)
			}
			continue
		}

		s.wg.Add(1)
		go s.deliver(ctx, provider, configured, claimed)
	}
	s.cursor = (start + 1) % len(providers)
}

func (s *Scheduler) deliver(ctx context.Context, provider *Provider, configured bool, task ClaimedNotification) {
	defer s.wg.Done()
	defer release(s.globalPermits)
	if configured {
		defer provider.Release()
	}

	outcome := DeliveryOutcome{
		Category:    ErrorProviderConfig,
		SafeMessage: "provider is missing or disabled",
	}
	if configured {
		outcome = provider.Deliver(ctx, task)
	}

	completedAt := s.clock.Now().UTC()
	var nextAttemptAt *time.Time
	if outcome.Retryable && task.AttemptInRound < task.MaxAttempts {
		delay := retryDelay(s.cfg.Retry, task.AttemptInRound, 0.5)
		if outcome.RetryAfter != nil {
			delay = boundedRetryAfter(*outcome.RetryAfter, s.cfg.Retry)
		}
		next := completedAt.Add(delay)
		nextAttemptAt = &next
	}
	err := s.store.Complete(context.WithoutCancel(ctx), CompleteRequest{
		ID:            task.ID,
		AttemptID:     task.AttemptID,
		WorkerID:      task.LeaseOwner,
		LeaseEpoch:    task.LeaseEpoch,
		Version:       task.Version,
		Outcome:       outcome,
		CompletedAt:   completedAt,
		NextAttemptAt: nextAttemptAt,
	})
	if err != nil && !errors.Is(err, ErrStaleLease) {
		s.log.Error("complete notification", "notification_id", task.ID, "error", err)
	}
}

func boundedRetryAfter(delay time.Duration, policy RetryConfig) time.Duration {
	if delay < policy.InitialDelay {
		delay = policy.InitialDelay
	}
	if policy.MaxDelay > 0 && delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}

func tryAcquire(semaphore chan struct{}) bool {
	select {
	case semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func release(semaphore chan struct{}) {
	<-semaphore
}
