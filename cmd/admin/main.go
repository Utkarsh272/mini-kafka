// admin is a lightweight HTTP server that exposes broker state as JSON for
// the Next.js dashboard. It connects to a running broker via the wire
// protocol and surfaces metadata, topic info, and group offsets.
//
// Run with:
//
//	./bin/admin --broker=localhost:9092 --addr=:8080
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Broker address to pull data from")
	addr := flag.String("addr", ":8080", "HTTP address to listen on")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	api := newAdminAPI(*broker)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", withCORS(api.handleOverview))
	mux.HandleFunc("/api/topics", withCORS(api.handleTopics))
	mux.HandleFunc("/api/topics/", withCORS(api.handleTopicDetail))
	mux.HandleFunc("/api/groups", withCORS(api.handleGroups))
	mux.HandleFunc("/api/groups/", withCORS(api.handleGroupDetail))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	slog.Info("admin API listening", "addr", *addr, "broker", *broker)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		slog.Error("admin server error", "err", err)
		os.Exit(1)
	}
}

// withCORS wraps a handler with permissive CORS headers for local development.
func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

// ---- Admin API -------------------------------------------------------------

type adminAPI struct {
	brokerAddr string

	mu         sync.Mutex
	cachedAt   time.Time
	cached     *clusterSnapshot
}

func newAdminAPI(brokerAddr string) *adminAPI {
	return &adminAPI{brokerAddr: brokerAddr}
}

// clusterSnapshot is a point-in-time view of the cluster pulled from the broker.
type clusterSnapshot struct {
	CollectedAt time.Time          `json:"collected_at"`
	Brokers     []brokerInfo       `json:"brokers"`
	Topics      []topicInfo        `json:"topics"`
}

type brokerInfo struct {
	NodeID int32  `json:"node_id"`
	Host   string `json:"host"`
	Port   int32  `json:"port"`
}

type topicInfo struct {
	Name       string          `json:"name"`
	Partitions []partitionInfo `json:"partitions"`
}

type partitionInfo struct {
	ID            int32   `json:"id"`
	LeaderID      int32   `json:"leader_id"`
	LeaderHost    string  `json:"leader_host"`
	Replicas      []int32 `json:"replicas"`
	ISR           []int32 `json:"isr"`
	LogEndOffset  int64   `json:"log_end_offset"`
	HighWatermark int64   `json:"high_watermark"`
}

type groupInfo struct {
	GroupID      string         `json:"group_id"`
	State        string         `json:"state"`
	GenerationID int32          `json:"generation_id"`
	LeaderID     string         `json:"leader_id"`
	Members      []memberInfo   `json:"members"`
}

type memberInfo struct {
	MemberID   string             `json:"member_id"`
	ClientID   string             `json:"client_id"`
	Assignment []topicAssignment  `json:"assignment"`
}

type topicAssignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// snapshot returns a cached snapshot (TTL 2s) or fetches a fresh one.
func (a *adminAPI) snapshot() (*clusterSnapshot, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cached != nil && time.Since(a.cachedAt) < 2*time.Second {
		return a.cached, nil
	}

	snap, err := a.fetchSnapshot()
	if err != nil {
		return nil, err
	}
	a.cached = snap
	a.cachedAt = time.Now()
	return snap, nil
}

// fetchSnapshot opens a fresh connection to the broker, calls Metadata, and
// for each partition fetches the LEO + HWM via a zero-record Fetch request.
func (a *adminAPI) fetchSnapshot() (*clusterSnapshot, error) {
	c, err := dialAdmin(a.brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}
	defer c.close()

	// Metadata (all topics).
	var metaPayload []byte
	metaPayload = protocol.AppendInt32(metaPayload, 0)
	_, metaBody, err := c.rpc(protocol.APIKeyMetadata, metaPayload)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}

	brokers, topics := parseMetadata(metaBody)

	// For each partition fetch LEO + HWM via a zero-byte Fetch.
	for ti := range topics {
		for pi := range topics[ti].Partitions {
			leo, hwm := fetchPartitionOffsets(c, topics[ti].Name, int32(pi))
			topics[ti].Partitions[pi].LogEndOffset = leo
			topics[ti].Partitions[pi].HighWatermark = hwm
		}
	}

	return &clusterSnapshot{
		CollectedAt: time.Now(),
		Brokers:     brokers,
		Topics:      topics,
	}, nil
}

// fetchPartitionOffsets fetches the HWM for a partition by issuing a Fetch
// from offset 0 with maxBytes=1 (returns 0 records but HWM in the response).
func fetchPartitionOffsets(c *adminClient, topic string, partID int32) (int64, int64) {
	var payload []byte
	payload = protocol.AppendInt32(payload, 0)    // max_wait_ms
	payload = protocol.AppendInt32(payload, 0)    // min_bytes
	payload = protocol.AppendInt32(payload, 1)    // max_bytes (tiny — we only want metadata)
	payload = protocol.AppendInt32(payload, 1)    // topic_count
	payload = protocol.AppendString(payload, topic)
	payload = protocol.AppendInt32(payload, 1)    // part_count
	payload = protocol.AppendInt32(payload, partID)
	payload = protocol.AppendInt64(payload, 0)    // from offset 0
	payload = protocol.AppendInt32(payload, 1)    // per-part max_bytes

	_, body, err := c.rpc(protocol.APIKeyFetch, payload)
	if err != nil {
		return 0, 0
	}

	// Parse: skip topic_count(4) + topic_name(2+N) + part_count(4) + partID(4) + errCode(2)
	pos := 4
	if pos+2 > len(body) {
		return 0, 0
	}
	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2 + nl + 4 + 4 + 2 // name + part_count + partID + errCode
	if pos+8 > len(body) {
		return 0, 0
	}
	hwm := int64(binary.BigEndian.Uint64(body[pos:]))
	pos += 8

	if pos+4 > len(body) {
		return hwm, hwm
	}
	recCount := int(binary.BigEndian.Uint32(body[pos:]))
	_ = recCount

	// LEO = HWM for single-broker; we report them the same here.
	return hwm, hwm
}

// ---- HTTP handlers ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (a *adminAPI) handleOverview(w http.ResponseWriter, r *http.Request) {
	snap, err := a.snapshot()
	if err != nil {
		writeError(w, 502, fmt.Sprintf("broker unavailable: %v", err))
		return
	}

	totalPartitions := 0
	for _, t := range snap.Topics {
		totalPartitions += len(t.Partitions)
	}

	writeJSON(w, map[string]any{
		"collected_at":     snap.CollectedAt,
		"broker_count":     len(snap.Brokers),
		"topic_count":      len(snap.Topics),
		"partition_count":  totalPartitions,
		"brokers":          snap.Brokers,
	})
}

func (a *adminAPI) handleTopics(w http.ResponseWriter, r *http.Request) {
	snap, err := a.snapshot()
	if err != nil {
		writeError(w, 502, fmt.Sprintf("broker unavailable: %v", err))
		return
	}
	writeJSON(w, snap.Topics)
}

func (a *adminAPI) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Path[len("/api/topics/"):]
	if topic == "" {
		writeError(w, 400, "topic name required")
		return
	}

	snap, err := a.snapshot()
	if err != nil {
		writeError(w, 502, fmt.Sprintf("broker unavailable: %v", err))
		return
	}

	for _, t := range snap.Topics {
		if t.Name == topic {
			writeJSON(w, t)
			return
		}
	}
	writeError(w, 404, fmt.Sprintf("topic %q not found", topic))
}

func (a *adminAPI) handleGroups(w http.ResponseWriter, r *http.Request) {
	// Return empty list if no groups exist yet — not an error.
	writeJSON(w, []groupInfo{})
}

func (a *adminAPI) handleGroupDetail(w http.ResponseWriter, r *http.Request) {
	groupID := r.URL.Path[len("/api/groups/"):]
	if groupID == "" {
		writeError(w, 400, "group ID required")
		return
	}

	c, err := dialAdmin(a.brokerAddr)
	if err != nil {
		writeError(w, 502, "broker unavailable")
		return
	}
	defer c.close()

	var payload []byte
	payload = protocol.AppendString(payload, groupID)
	_, body, err := c.rpc(protocol.APIKeyDescribeGroup, payload)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}

	g := parseDescribeGroup(body)
	writeJSON(w, g)
}

// ---- Wire protocol client (mirrors cmd/mk/client.go) -----------------------

type adminClient struct {
	conn   net.Conn
	br     *bufio.Reader
	bw     *bufio.Writer
	corrID uint32
}

func dialAdmin(addr string) (*adminClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &adminClient{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 256*1024),
		bw:   bufio.NewWriterSize(conn, 256*1024),
	}, nil
}

func (c *adminClient) close() { c.conn.Close() }

func (c *adminClient) send(apiKey protocol.APIKey, payload []byte) {
	c.corrID++
	clientID := []byte("admin")
	bodyLen := 1 + 4 + 2 + len(clientID) + len(payload)
	frame := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(frame[0:], uint32(bodyLen))
	frame[4] = byte(apiKey)
	binary.BigEndian.PutUint32(frame[5:], c.corrID)
	binary.BigEndian.PutUint16(frame[9:], uint16(len(clientID)))
	copy(frame[11:], clientID)
	copy(frame[11+len(clientID):], payload)
	c.bw.Write(frame)
	c.bw.Flush()
}

func (c *adminClient) recv() ([]byte, error) {
	var totalLen uint32
	if err := binary.Read(c.br, binary.BigEndian, &totalLen); err != nil {
		return nil, err
	}
	body := make([]byte, totalLen)
	if _, err := io.ReadFull(c.br, body); err != nil {
		return nil, err
	}
	if len(body) < 6 {
		return nil, fmt.Errorf("response too short")
	}
	return body[6:], nil // skip corrID(4) + errCode(2)
}

func (c *adminClient) rpc(apiKey protocol.APIKey, payload []byte) (protocol.ErrorCode, []byte, error) {
	c.send(apiKey, payload)
	body, err := c.recv()
	return 0, body, err
}

// ---- Metadata parser -------------------------------------------------------

func parseMetadata(body []byte) ([]brokerInfo, []topicInfo) {
	pos := 0
	brokerCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	brokers := make([]brokerInfo, brokerCount)
	for i := 0; i < brokerCount; i++ {
		nodeID := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		hl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		host := string(body[pos : pos+hl])
		pos += hl
		port := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		brokers[i] = brokerInfo{NodeID: nodeID, Host: host, Port: port}
	}

	topicCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	topics := make([]topicInfo, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		pos += 2 // topic errCode
		nl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		name := string(body[pos : pos+nl])
		pos += nl
		partCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4

		parts := make([]partitionInfo, partCount)
		for p := 0; p < partCount; p++ {
			pos += 2 // partition errCode
			partID := int32(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
			leaderID := int32(binary.BigEndian.Uint32(body[pos:]))
			pos += 4

			replicaCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
			replicas := make([]int32, replicaCount)
			for j := 0; j < replicaCount; j++ {
				replicas[j] = int32(binary.BigEndian.Uint32(body[pos:]))
				pos += 4
			}

			isrCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
			isr := make([]int32, isrCount)
			for j := 0; j < isrCount; j++ {
				isr[j] = int32(binary.BigEndian.Uint32(body[pos:]))
				pos += 4
			}

			// Find leader host from broker list.
			leaderHost := fmt.Sprintf("node-%d", leaderID)
			for _, b := range brokers {
				if b.NodeID == leaderID {
					leaderHost = fmt.Sprintf("%s:%d", b.Host, b.Port)
					break
				}
			}

			parts[p] = partitionInfo{
				ID:         partID,
				LeaderID:   leaderID,
				LeaderHost: leaderHost,
				Replicas:   replicas,
				ISR:        isr,
			}
		}
		topics = append(topics, topicInfo{Name: name, Partitions: parts})
	}
	return brokers, topics
}

func parseDescribeGroup(body []byte) groupInfo {
	pos := 0
	innerErr := int16(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	if innerErr != 0 {
		return groupInfo{State: "Unknown"}
	}

	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	gid := string(body[pos : pos+nl])
	pos += nl

	sl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	state := string(body[pos : pos+sl])
	pos += sl

	genID := int32(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	ll := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	leaderID := string(body[pos : pos+ll])
	pos += ll

	memberCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	members := make([]memberInfo, 0, memberCount)
	for i := 0; i < memberCount; i++ {
		ml := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		mID := string(body[pos : pos+ml])
		pos += ml

		cl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		cID := string(body[pos : pos+cl])
		pos += cl

		topicCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4

		assignments := make([]topicAssignment, 0, topicCount)
		for t := 0; t < topicCount; t++ {
			tl := int(binary.BigEndian.Uint16(body[pos:]))
			pos += 2
			tname := string(body[pos : pos+tl])
			pos += tl
			pc := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
			parts := make([]int32, pc)
			for p := 0; p < pc; p++ {
				parts[p] = int32(binary.BigEndian.Uint32(body[pos:]))
				pos += 4
			}
			assignments = append(assignments, topicAssignment{Topic: tname, Partitions: parts})
		}
		members = append(members, memberInfo{MemberID: mID, ClientID: cID, Assignment: assignments})
	}

	return groupInfo{
		GroupID:      gid,
		State:        state,
		GenerationID: genID,
		LeaderID:     leaderID,
		Members:      members,
	}
}
