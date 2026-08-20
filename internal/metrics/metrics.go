package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Enqueued       *prometheus.CounterVec
	Succeeded      *prometheus.CounterVec
	Failed         *prometheus.CounterVec
	Dead           *prometheus.CounterVec
	Retries        *prometheus.CounterVec
	ProcessingTime *prometheus.HistogramVec

	Registry *prometheus.Registry
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_enqueued_total",
			Help: "Number of jobs enqueued, by queue.",
		}, []string{"queue"}),
		Succeeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_succeeded_total",
			Help: "Number of jobs processed successfully, by type.",
		}, []string{"type"}),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Number of job attempts that failed, by type.",
		}, []string{"type"}),
		Dead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_dead_total",
			Help: "Number of jobs moved to the DLQ, by type.",
		}, []string{"type"}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Number of jobs scheduled for retry, by type.",
		}, []string{"type"}),
		ProcessingTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_processing_seconds",
			Help:    "Duration of job processing, by type.",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		Registry: reg,
	}
	reg.MustRegister(m.Enqueued, m.Succeeded, m.Failed, m.Dead, m.Retries, m.ProcessingTime)
	return m
}
