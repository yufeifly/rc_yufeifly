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
	metrics   *Metrics
	log       *slog.Logger
	clock     Clock
	workerID  string

	globalPermits chan struct{}
	wake          chan struct{}

	mu           sync.Mutex
	claimCancel  context.CancelFunc
	workerCancel context.CancelFunc
	cursor       int
	loopWG       sync.WaitGroup
	workerWG     sync.WaitGroup
}

func NewScheduler(cfg Config, store *Store, providers *ProviderRegistry, metrics *Metrics, log *slog.Logger) *Scheduler {
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
		metrics:       metrics,
		log:           log,
		clock:         systemClock{},
		workerID:      "worker_" + ulid.Make().String(),
		globalPermits: make(chan struct{}, concurrency),
		wake:          make(chan struct{}, 1),
	}
}

func (s *Scheduler) Start(parent context.Context) {
	s.mu.Lock()
	if s.claimCancel != nil {
		s.mu.Unlock()
		return
	}
	claimContext, claimCancel := context.WithCancel(parent)
	workerContext, workerCancel := context.WithCancel(context.Background())
	s.claimCancel = claimCancel
	s.workerCancel = workerCancel
	s.loopWG.Add(1)
	s.mu.Unlock()

	go s.run(claimContext, workerContext)
}

func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	claimCancel := s.claimCancel
	workerCancel := s.workerCancel
	s.mu.Unlock()
	if claimCancel != nil {
		claimCancel()
	}
	s.loopWG.Wait()

	done := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		if workerCancel != nil {
			workerCancel()
		}
		return nil
	case <-ctx.Done():
		if workerCancel != nil {
			workerCancel()
		}
		return ctx.Err()
	}
}

func (s *Scheduler) run(ctx, workerContext context.Context) {
	defer s.loopWG.Done()
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

		s.dispatch(ctx, workerContext)
		timer.Reset(pollInterval)
	}
}

func (s *Scheduler) dispatch(ctx, workerContext context.Context) {
	now := s.clock.Now().UTC()
	recoveryLimit := s.cfg.Scheduler.RecoveryBatchSize
	if recoveryLimit <= 0 {
		recoveryLimit = 100
	}
	databaseStartedAt := time.Now()
	_, err := s.store.RecoverExpired(ctx, now, recoveryLimit)
	s.metrics.ObserveDatabase("recover_expired", time.Since(databaseStartedAt), err)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Error("recover expired leases", "error", err)
		}
		return
	}
	databaseStartedAt = time.Now()
	providers, err := s.store.ListDueProviders(ctx, now, s.cfg.Priority.AgingInterval)
	s.metrics.ObserveDatabase("list_due_providers", time.Since(databaseStartedAt), err)
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
		if ctx.Err() != nil {
			return
		}
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
		databaseStartedAt = time.Now()
		claimed, claimErr := s.store.ClaimAndStart(ctx, ClaimRequest{
			ProviderID:      due.ProviderID,
			ProviderVersion: providerVersion,
			WorkerID:        s.workerID,
			Now:             now,
			LeaseDuration:   s.cfg.Scheduler.LeaseDuration,
			AgingInterval:   s.cfg.Priority.AgingInterval,
		})
		s.metrics.ObserveDatabase("claim_and_start", time.Since(databaseStartedAt), claimErr)
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

		s.metrics.ObserveClaim(claimed.ProviderID, claimed.Priority)
		s.workerWG.Add(1)
		go s.deliver(workerContext, provider, configured, claimed)
	}
	s.cursor = (start + 1) % len(providers)
}

func (s *Scheduler) deliver(ctx context.Context, provider *Provider, configured bool, task ClaimedNotification) {
	defer s.workerWG.Done()
	defer release(s.globalPermits)
	if configured {
		defer provider.Release()
	}
	defer s.metrics.ObserveWorkerReleased(task.ProviderID)

	startedAt := s.clock.Now()
	outcome := DeliveryOutcome{
		Category:    ErrorProviderConfig,
		SafeMessage: "provider is missing or disabled",
	}
	if configured {
		outcome = provider.Deliver(ctx, task)
	}
	s.metrics.ObserveDelivery(task.ProviderID, outcome, s.clock.Now().Sub(startedAt))

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
	request := CompleteRequest{
		ID:            task.ID,
		AttemptID:     task.AttemptID,
		WorkerID:      task.LeaseOwner,
		LeaseEpoch:    task.LeaseEpoch,
		Version:       task.Version,
		Outcome:       outcome,
		CompletedAt:   completedAt,
		NextAttemptAt: nextAttemptAt,
	}
	err := s.completeWithRetry(request)
	if errors.Is(err, ErrStaleLease) {
		s.metrics.ObserveStaleCompletion(task.ProviderID)
	} else if err != nil {
		s.log.Error("complete notification", "notification_id", task.ID, "error", err)
	} else if nextAttemptAt != nil {
		s.metrics.ObserveRetry(task.ProviderID, outcome.Category)
	} else if !outcome.Success {
		s.metrics.ObserveDead(task.ProviderID, outcome.Category)
	}
}

func (s *Scheduler) completeWithRetry(request CompleteRequest) error {
	budget := s.cfg.Scheduler.FinalizeRetryBudget
	if budget <= 0 {
		budget = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	delay := 25 * time.Millisecond
	for {
		startedAt := time.Now()
		err := s.store.Complete(ctx, request)
		s.metrics.ObserveDatabase("complete", time.Since(startedAt), err)
		if err == nil || errors.Is(err, ErrStaleLease) {
			return err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if delay < 250*time.Millisecond {
			delay *= 2
		}
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
