// Package metrics defines all Prometheus metrics for the mini-kafka broker.
// Call Register() once at broker startup, then use the exported vars freely
// from any package.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---- Request metrics -------------------------------------------------------

// RequestsTotal counts every handled request by api_key and result.
var RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mini_kafka",
	Name:      "requests_total",
	Help:      "Total number of broker requests handled.",
}, []string{"api_key", "result"}) // result: ok | error

// RequestDurationMs tracks request latency by api_key.
var RequestDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "mini_kafka",
	Name:      "request_duration_ms",
	Help:      "Request handling latency in milliseconds.",
	Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500},
}, []string{"api_key"})

// ---- Produce / Fetch metrics -----------------------------------------------

// MessagesProducedTotal counts records successfully appended.
var MessagesProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mini_kafka",
	Name:      "messages_produced_total",
	Help:      "Total records successfully written to partition logs.",
}, []string{"topic", "partition"})

// MessagesFetchedTotal counts records returned to consumers.
var MessagesFetchedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mini_kafka",
	Name:      "messages_fetched_total",
	Help:      "Total records returned to Fetch requests.",
}, []string{"topic", "partition"})

// BytesProducedTotal tracks produce throughput in bytes.
var BytesProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mini_kafka",
	Name:      "bytes_produced_total",
	Help:      "Total bytes written to partition logs.",
}, []string{"topic"})

// ---- Storage metrics -------------------------------------------------------

// PartitionLogEndOffset is a gauge reflecting each partition's LEO.
// Updated on every successful Append.
var PartitionLogEndOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "partition_log_end_offset",
	Help:      "Current log end offset (next offset to be written) per partition.",
}, []string{"topic", "partition"})

// PartitionHighWatermark is a gauge reflecting each partition's HWM.
var PartitionHighWatermark = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "partition_high_watermark",
	Help:      "Current high-watermark offset per partition.",
}, []string{"topic", "partition"})

// PartitionReplicationLag is LEO - HWM (0 for single-broker).
var PartitionReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "partition_replication_lag",
	Help:      "Records between LEO and high-watermark (replication lag).",
}, []string{"topic", "partition"})

// ---- ISR metrics -----------------------------------------------------------

// ISRSize tracks the number of in-sync replicas per partition.
var ISRSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "isr_size",
	Help:      "Number of in-sync replicas per partition.",
}, []string{"topic", "partition"})

// ---- Consumer group metrics ------------------------------------------------

// ConsumerGroupLag tracks the message lag per (group, topic, partition).
var ConsumerGroupLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "consumer_group_lag",
	Help:      "Message lag (LEO - committed offset) per consumer group partition.",
}, []string{"group", "topic", "partition"})

// OffsetCommitsTotal counts successful OffsetCommit calls.
var OffsetCommitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mini_kafka",
	Name:      "offset_commits_total",
	Help:      "Total successful offset commits.",
}, []string{"group"})

// ---- Connection metrics ----------------------------------------------------

// ActiveConnections tracks the number of live TCP connections.
var ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "mini_kafka",
	Name:      "active_connections",
	Help:      "Number of currently active TCP client connections.",
})

// ---- Helpers ---------------------------------------------------------------

// Handler returns the Prometheus HTTP handler for the /metrics endpoint.
func Handler() http.Handler {
	return promhttp.Handler()
}

// APIKeyName maps protocol API key numbers to human-readable names.
// Avoids importing the protocol package into metrics (would create a cycle).
func APIKeyName(key uint8) string {
	switch key {
	case 0:
		return "produce"
	case 1:
		return "fetch"
	case 2:
		return "metadata"
	case 3:
		return "join_group"
	case 4:
		return "sync_group"
	case 5:
		return "heartbeat"
	case 6:
		return "offset_commit"
	case 7:
		return "offset_fetch"
	case 8:
		return "fetch_follower"
	case 9:
		return "leave_group"
	case 10:
		return "create_topic"
	case 11:
		return "describe_group"
	default:
		return "unknown"
	}
}
