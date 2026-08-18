package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusScheduled Status = "scheduled"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

type Job struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Queue      string          `json:"queue"`
	Payload    json.RawMessage `json:"payload"`
	Status     Status          `json:"status"`
	Priority   int             `json:"priority"`
	Attempt    int             `json:"attempt"`
	MaxRetries int             `json:"max_retries"`
	RunAfter   time.Time       `json:"run_after"`
	LeaseUntil time.Time       `json:"lease_until"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	Error      string          `json:"error,omitempty"`
	DedupKey   string          `json:"dedup_key,omitempty"`
}

func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
