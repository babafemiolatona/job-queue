package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"job-queue/internal/broker"
)

type queueCollector struct {
	getStats func(context.Context) (*broker.Stats, error)

	queueDepth    *prometheus.GaugeVec
	queueInFlight *prometheus.GaugeVec
	dlqDepth      prometheus.Gauge
	delayed       prometheus.Gauge
	retry         prometheus.Gauge
}

func NewQueueCollector(getStats func(context.Context) (*broker.Stats, error)) prometheus.Collector {
	return &queueCollector{
		getStats: getStats,
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Number of ready messages in the stream, by queue.",
		}, []string{"queue"}),
		queueInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_in_flight",
			Help: "Number of messages pending in consumer groups (PEL), by queue.",
		}, []string{"queue"}),
		dlqDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_dlq_depth",
			Help: "Number of messages in the DLQ stream.",
		}),
		delayed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_delayed",
			Help: "Number of delayed jobs waiting to run.",
		}),
		retry: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_retry",
			Help: "Number of jobs waiting for retry.",
		}),
	}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	c.queueDepth.Describe(ch)
	c.queueInFlight.Describe(ch)
	c.dlqDepth.Describe(ch)
	c.delayed.Describe(ch)
	c.retry.Describe(ch)
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	if stats, err := c.getStats(context.Background()); err == nil {
		for _, q := range stats.Queues {
			c.queueDepth.WithLabelValues(q.Queue).Set(float64(q.Ready))
			c.queueInFlight.WithLabelValues(q.Queue).Set(float64(q.InFlight))
		}
		c.dlqDepth.Set(float64(stats.DLQ))
		c.delayed.Set(float64(stats.Delayed))
		c.retry.Set(float64(stats.Retry))
	}
	c.queueDepth.Collect(ch)
	c.queueInFlight.Collect(ch)
	c.dlqDepth.Collect(ch)
	c.delayed.Collect(ch)
	c.retry.Collect(ch)
}
