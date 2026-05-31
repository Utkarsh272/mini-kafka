// bench is a throughput benchmark that measures sustained produce RPS against a
// running broker. Run with:
//
//	go run ./scripts/bench/main.go --addr=localhost:9092 --messages=100000
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

func main() {
	addr := flag.String("addr", "localhost:9092", "Broker address")
	messages := flag.Int("messages", 100_000, "Total messages to produce")
	batchSize := flag.Int("batch", 100, "Records per Produce request")
	valueSize := flag.Int("value-size", 128, "Value size in bytes (approximate)")
	flag.Parse()

	fmt.Printf("==> Benchmark: %d messages, batch=%d, value=%dB → %s\n",
		*messages, *batchSize, *valueSize, *addr)

	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 256*1024)
	bw := bufio.NewWriterSize(conn, 256*1024)

	// Create the bench topic (ignore error if already exists).
	{
		var buf []byte
		buf = protocol.AppendString(buf, "bench-topic")
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendInt32(buf, 1)
		sendFrame(bw, protocol.APIKeyCreateTopic, 1, buf)
		bw.Flush()
		readResponse(br)
	}

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = 'x'
	}

	var corrID uint32 = 1
	sent := 0
	start := time.Now()

	for sent < *messages {
		batch := *batchSize
		if sent+batch > *messages {
			batch = *messages - sent
		}

		corrID++

		var payload []byte
		payload = protocol.AppendInt16(payload, 1)   // acks=1
		payload = protocol.AppendInt32(payload, 500) // timeout_ms
		payload = protocol.AppendInt32(payload, 1)   // topic_count
		payload = protocol.AppendString(payload, "bench-topic")
		payload = protocol.AppendInt32(payload, 1) // part_count
		payload = protocol.AppendInt32(payload, 0) // partition 0
		payload = protocol.AppendInt32(payload, int32(batch))
		for i := 0; i < batch; i++ {
			payload = protocol.AppendBytes(payload, nil)   // null key
			payload = protocol.AppendBytes(payload, value) // value
		}

		sendFrame(bw, protocol.APIKeyProduce, corrID, payload)
		bw.Flush()
		readResponse(br)

		sent += batch
	}

	elapsed := time.Since(start)
	rps := float64(*messages) / elapsed.Seconds()
	mbps := float64(*messages) * float64(*valueSize) / elapsed.Seconds() / (1024 * 1024)

	fmt.Printf("\n==> Results\n")
	fmt.Printf("    Messages:  %d\n", *messages)
	fmt.Printf("    Elapsed:   %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("    Throughput: %.0f msg/sec\n", rps)
	fmt.Printf("    Bandwidth:  %.1f MB/sec\n", mbps)
}

func sendFrame(w *bufio.Writer, apiKey protocol.APIKey, corrID uint32, payload []byte) {
	clientID := []byte("bench")
	bodyLen := 1 + 4 + 2 + len(clientID) + len(payload)
	frame := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(frame[0:], uint32(bodyLen))
	frame[4] = byte(apiKey)
	binary.BigEndian.PutUint32(frame[5:], corrID)
	binary.BigEndian.PutUint16(frame[9:], uint16(len(clientID)))
	copy(frame[11:], clientID)
	copy(frame[11+len(clientID):], payload)
	w.Write(frame)
}

func readResponse(r *bufio.Reader) {
	var totalLen uint32
	binary.Read(r, binary.BigEndian, &totalLen)
	body := make([]byte, totalLen)
	r.Read(body)
}
