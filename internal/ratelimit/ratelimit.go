package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter provides thread-safe rate limiting using token bucket algorithm
type RateLimiter struct {
	limiter *rate.Limiter
	mu      sync.RWMutex
	stats   Stats
}

// Stats holds rate limiter statistics
type Stats struct {
	TotalRequests   int64     `json:"total_requests"`
	AllowedRequests int64     `json:"allowed_requests"`
	DeniedRequests  int64     `json:"denied_requests"`
	LastRequest     time.Time `json:"last_request"`
}

// New creates a new rate limiter
func New(requestsPerSecond float64, burstSize int) *RateLimiter {
	return &RateLimiter{
		limiter: rate.NewLimiter(rate.Limit(requestsPerSecond), burstSize),
		stats:   Stats{},
	}
}

// Allow checks if a request is allowed immediately
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.stats.TotalRequests++
	rl.stats.LastRequest = time.Now()

	allowed := rl.limiter.Allow()
	if allowed {
		rl.stats.AllowedRequests++
	} else {
		rl.stats.DeniedRequests++
	}

	return allowed
}

// Wait blocks until a request is allowed or context is cancelled
func (rl *RateLimiter) Wait(ctx context.Context) error {
	rl.mu.Lock()
	rl.stats.TotalRequests++
	rl.stats.LastRequest = time.Now()
	rl.mu.Unlock()

	err := rl.limiter.Wait(ctx)

	rl.mu.Lock()
	if err == nil {
		rl.stats.AllowedRequests++
	} else {
		rl.stats.DeniedRequests++
	}
	rl.mu.Unlock()

	return err
}

// WaitN blocks until n requests are allowed or context is cancelled
func (rl *RateLimiter) WaitN(ctx context.Context, n int) error {
	rl.mu.Lock()
	rl.stats.TotalRequests += int64(n)
	rl.stats.LastRequest = time.Now()
	rl.mu.Unlock()

	err := rl.limiter.WaitN(ctx, n)

	rl.mu.Lock()
	if err == nil {
		rl.stats.AllowedRequests += int64(n)
	} else {
		rl.stats.DeniedRequests += int64(n)
	}
	rl.mu.Unlock()

	return err
}

// Reserve reserves a token for future use
func (rl *RateLimiter) Reserve() *rate.Reservation {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.stats.TotalRequests++
	rl.stats.LastRequest = time.Now()

	reservation := rl.limiter.Reserve()
	if reservation.OK() {
		rl.stats.AllowedRequests++
	} else {
		rl.stats.DeniedRequests++
	}

	return reservation
}

// GetStats returns current rate limiter statistics
func (rl *RateLimiter) GetStats() Stats {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.stats
}

// GetStatus returns current rate limiter status
func (rl *RateLimiter) GetStatus() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	burst := rl.limiter.Burst()
	tokens := rl.limiter.Tokens()
	limit := float64(rl.limiter.Limit())

	return map[string]interface{}{
		"limit":            limit,
		"burst":            burst,
		"available_tokens": tokens,
		"utilization":      (float64(burst) - tokens) / float64(burst),
		"stats":            rl.stats,
	}
}

// SetLimit updates the rate limit
func (rl *RateLimiter) SetLimit(requestsPerSecond float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limiter.SetLimit(rate.Limit(requestsPerSecond))
}

// SetBurst updates the burst size
func (rl *RateLimiter) SetBurst(burstSize int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limiter.SetBurst(burstSize)
}
