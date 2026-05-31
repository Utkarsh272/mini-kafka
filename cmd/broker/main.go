package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Utkarsh272/mini-kafka/internal/broker"
	"github.com/Utkarsh272/mini-kafka/internal/server"
)

func main() {
	addr := flag.String("addr", ":9092", "TCP address to listen on")
	dataDir := flag.String("data-dir", "/tmp/mini-kafka", "Root directory for partition logs and metadata db")
	nodeID := flag.Int("node-id", 1, "Broker node ID (must be unique in cluster)")
	host := flag.String("host", "localhost", "Advertised hostname returned in Metadata responses")
	port := flag.Int("port", 9092, "Advertised port returned in Metadata responses")
	logLevel := flag.String("log-level", "info", "Log level: debug|info|warn|error")

	// Cluster peers: comma-separated list of "nodeID:host:port" for other brokers.
	// Used to wire follower fetchers on startup for topics with RF > 1.
	// Example: --peers=2:broker-2:9093,3:broker-3:9094
	peers := flag.String("peers", "", "Cluster peer list: nodeID:host:port,...")

	flag.Parse()

	// Configure structured logging.
	var level slog.Level
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		slog.Error("create data dir", "err", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(int32(*nodeID), *host, int32(*port), *dataDir)
	if err != nil {
		slog.Error("init broker", "err", err)
		os.Exit(1)
	}
	defer b.Close()

	// Wire cluster peers for replication if provided.
	if *peers != "" {
		if err := wirePeers(b, *peers); err != nil {
			slog.Warn("peer wiring partial failure", "err", err)
			// Non-fatal: broker can still serve producers and consumers.
		}
	}

	h := server.NewHandler(b)
	srv := server.NewServer(*addr, h)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutting down broker")
		srv.Close()
	}()

	slog.Info("mini-kafka broker started",
		"node_id", *nodeID,
		"addr", *addr,
		"host", *host,
		"port", *port,
		"data_dir", *dataDir,
		"peers", *peers,
	)

	if err := srv.ListenAndServe(); err != nil {
		slog.Error("broker error", "err", err)
		os.Exit(1)
	}
}

// peer holds the parsed address of one cluster peer.
type peer struct {
	nodeID int32
	host   string
	port   int32
}

// parsePeers parses the --peers flag into a list of peer structs.
// Format: "nodeID:host:port,nodeID:host:port"
func parsePeers(raw string) ([]peer, error) {
	var peers []peer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid peer format %q (want nodeID:host:port)", entry)
		}
		nid, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid nodeID in peer %q: %w", entry, err)
		}
		p, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid port in peer %q: %w", entry, err)
		}
		peers = append(peers, peer{
			nodeID: int32(nid),
			host:   parts[1],
			port:   int32(p),
		})
	}
	return peers, nil
}

// wirePeers sets up follower fetchers for all topics where this broker
// is a follower (i.e. the partition's leader is a different node).
//
// For a portfolio-grade single-binary deployment, we use a simple static
// leader assignment: partition i is led by node ((i % len(brokers)) + 1).
// This is documented in DESIGN.md as a known simplification vs KRaft/ZooKeeper.
func wirePeers(b *broker.Broker, rawPeers string) error {
	clusterPeers, err := parsePeers(rawPeers)
	if err != nil {
		return fmt.Errorf("parse peers: %w", err)
	}

	// Build nodeID → addr map (includes this broker's peers only).
	addrByNodeID := make(map[int32]string)
	for _, p := range clusterPeers {
		addrByNodeID[p.nodeID] = fmt.Sprintf("%s:%d", p.host, p.port)
	}

	// Total cluster size = peers + self.
	clusterSize := int32(len(clusterPeers) + 1)

	myNodeID := b.NodeID()

	for _, topic := range b.ListTopics() {
		rf := topic.ReplicationFactor()
		if rf <= 1 {
			continue // no replication needed
		}

		numPartitions := int32(topic.NumPartitions())
		for partID := int32(0); partID < numPartitions; partID++ {
			// Static leader: node ((partID % clusterSize) + 1)
			leaderNodeID := (partID%clusterSize + 1)

			if leaderNodeID == myNodeID {
				// We are the leader — attach ISR tracker.
				// Replicas are this node + the next (rf-1) nodes in the ring.
				replicas := make([]int32, 0, rf)
				for i := int32(0); i < rf; i++ {
					replica := ((partID+i)%clusterSize + 1)
					replicas = append(replicas, replica)
				}
				if err := b.InitLeaderISR(topic.Name(), partID, replicas); err != nil {
					slog.Warn("init leader ISR failed",
						"topic", topic.Name(), "partition", partID, "err", err)
				} else {
					slog.Info("leader ISR initialised",
						"topic", topic.Name(), "partition", partID,
						"replicas", replicas)
				}
			} else {
				// We are a follower — start fetcher pointed at the leader.
				leaderAddr, ok := addrByNodeID[leaderNodeID]
				if !ok {
					slog.Warn("leader addr unknown, skipping follower fetch",
						"topic", topic.Name(), "partition", partID,
						"leader_node_id", leaderNodeID)
					continue
				}
				if err := b.InitFollowerFetcher(topic.Name(), partID, leaderAddr); err != nil {
					slog.Warn("init follower fetcher failed",
						"topic", topic.Name(), "partition", partID, "err", err)
				} else {
					slog.Info("follower fetcher started",
						"topic", topic.Name(), "partition", partID,
						"leader", leaderAddr)
				}
			}
		}
	}
	return nil
}
