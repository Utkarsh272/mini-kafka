// smoke_test is a standalone client that exercises the full mini-kafka wire
// protocol against a running broker. Run with:
//
//	go run ./scripts/smoke_test/main.go --addr=localhost:9092
//
// Exit code 0 = all checks passed. Exit code 1 = at least one check failed.
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
	flag.Parse()

	fmt.Printf("==> Smoke test against %s\n", *addr)

	conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	var corrID uint32
	failed := false

	send := func(apiKey protocol.APIKey, payload []byte) uint32 {
		corrID++
		id := corrID
		clientID := []byte("smoke-test")
		bodyLen := 1 + 4 + 2 + len(clientID) + len(payload)
		frame := make([]byte, 4+bodyLen)
		binary.BigEndian.PutUint32(frame[0:], uint32(bodyLen))
		frame[4] = byte(apiKey)
		binary.BigEndian.PutUint32(frame[5:], id)
		binary.BigEndian.PutUint16(frame[9:], uint16(len(clientID)))
		copy(frame[11:], clientID)
		copy(frame[11+len(clientID):], payload)
		bw.Write(frame)
		bw.Flush()
		return id
	}

	recv := func() (uint32, protocol.ErrorCode, []byte) {
		var totalLen uint32
		binary.Read(br, binary.BigEndian, &totalLen)
		body := make([]byte, totalLen)
		br.Read(body)
		corrID := binary.BigEndian.Uint32(body[0:])
		errCode := protocol.ErrorCode(int16(binary.BigEndian.Uint16(body[4:])))
		return corrID, errCode, body[6:]
	}

	check := func(name string, cond bool) {
		if cond {
			fmt.Printf("  ✓ %s\n", name)
		} else {
			fmt.Printf("  ✗ %s\n", name)
			failed = true
		}
	}

	// 1. CreateTopic
	var buf []byte
	buf = protocol.AppendString(buf, "smoke-topic")
	buf = protocol.AppendInt32(buf, 3) // 3 partitions
	buf = protocol.AppendInt32(buf, 1) // RF=1
	send(protocol.APIKeyCreateTopic, buf)
	_, errCode, payload := recv()
	nameLen := int(binary.BigEndian.Uint16(payload[0:]))
	innerErr := protocol.ErrorCode(int16(binary.BigEndian.Uint16(payload[2+nameLen:])))
	check("CreateTopic smoke-topic (3 partitions)", errCode == protocol.ErrNone && innerErr == protocol.ErrNone)

	// 2. Metadata
	buf = nil
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, "smoke-topic")
	send(protocol.APIKeyMetadata, buf)
	_, errCode, payload = recv()
	check("Metadata returned ErrNone", errCode == protocol.ErrNone)
	// Quick parse: skip brokers, check topic count
	pos := 0
	brokerCount := int(binary.BigEndian.Uint32(payload[pos:]))
	pos += 4
	for i := 0; i < brokerCount; i++ {
		pos += 4
		hl := int(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2 + hl + 4
	}
	topicCount := int(binary.BigEndian.Uint32(payload[pos:]))
	check("Metadata returns 1 topic", topicCount == 1)

	// 3. Produce to each partition
	for partID := int32(0); partID < 3; partID++ {
		buf = nil
		buf = protocol.AppendInt16(buf, 1)
		buf = protocol.AppendInt32(buf, 500)
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendString(buf, "smoke-topic")
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendInt32(buf, partID)
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendBytes(buf, []byte(fmt.Sprintf("key-%d", partID)))
		buf = protocol.AppendBytes(buf, []byte(fmt.Sprintf("value-%d", partID)))
		send(protocol.APIKeyProduce, buf)
		_, errCode, _ = recv()
		check(fmt.Sprintf("Produce to partition %d", partID), errCode == protocol.ErrNone)
	}

	// 4. Fetch from each partition
	for partID := int32(0); partID < 3; partID++ {
		buf = nil
		buf = protocol.AppendInt32(buf, 500)
		buf = protocol.AppendInt32(buf, 0)
		buf = protocol.AppendInt32(buf, 1<<20)
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendString(buf, "smoke-topic")
		buf = protocol.AppendInt32(buf, 1)
		buf = protocol.AppendInt32(buf, partID)
		buf = protocol.AppendInt64(buf, 0)
		buf = protocol.AppendInt32(buf, 1<<20)
		send(protocol.APIKeyFetch, buf)
		_, errCode, payload = recv()
		check(fmt.Sprintf("Fetch returns ErrNone for partition %d", partID), errCode == protocol.ErrNone)

		// Quick parse: verify 1 record in this partition
		p := 4
		nl := int(binary.BigEndian.Uint16(payload[p:]))
		p += 2 + nl + 4 + 4 + 2 + 8
		recCount := int32(binary.BigEndian.Uint32(payload[p:]))
		check(fmt.Sprintf("Partition %d has 1 record", partID), recCount == 1)
	}

	// 5. OffsetCommit + OffsetFetch
	buf = nil
	buf = protocol.AppendString(buf, "smoke-group")
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, "smoke-topic")
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, 0)
	buf = protocol.AppendInt64(buf, 0)
	buf = protocol.AppendString(buf, "")
	send(protocol.APIKeyOffsetCommit, buf)
	_, errCode, _ = recv()
	check("OffsetCommit", errCode == protocol.ErrNone)

	buf = nil
	buf = protocol.AppendString(buf, "smoke-group")
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendString(buf, "smoke-topic")
	buf = protocol.AppendInt32(buf, 1)
	buf = protocol.AppendInt32(buf, 0)
	send(protocol.APIKeyOffsetFetch, buf)
	_, errCode, payload = recv()
	pos = 4
	nl := int(binary.BigEndian.Uint16(payload[pos:]))
	pos += 2 + nl + 4 + 4
	committed := int64(binary.BigEndian.Uint64(payload[pos:]))
	check("OffsetFetch returns committed offset 0", errCode == protocol.ErrNone && committed == 0)

	fmt.Println()
	if failed {
		fmt.Println("==> FAIL: one or more checks failed")
		os.Exit(1)
	}
	fmt.Println("==> PASS: all checks passed")
}
