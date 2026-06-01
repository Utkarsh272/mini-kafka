package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

// cmdProduce handles: mk produce [flags] <topic>
//
// Flags must come before the topic name (standard Go flag convention).
// Examples:
//
//	mk produce orders --key user-1 --value "hello"
//	mk produce orders --value "hello" --count 5
//	mk produce orders                              # reads from stdin
func cmdProduce(args []string, addr string) error {
	fs := newFlagSet("mk produce [--key K] [--value V] [--count N] [--partition N] <topic>")
	partition := fs.Int("partition", -1, "Target partition (-1 = auto-route by key)")
	key := fs.String("key", "", "Message key (empty = keyless → round-robin routing)")
	value := fs.String("value", "", "Message value (if empty, reads lines from stdin)")
	count := fs.Int("count", 1, "Number of times to send --value (ignored in stdin mode)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: mk produce [--key K] [--value V] [--count N] [--partition N] <topic>")
	}
	topic := fs.Arg(0)

	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	send := func(k, v string) error {
		var keyBytes []byte
		if k != "" {
			keyBytes = []byte(k)
		}

		var payload []byte
		payload = protocol.AppendInt16(payload, 1)   // acks=1
		payload = protocol.AppendInt32(payload, 500) // timeout_ms
		payload = protocol.AppendInt32(payload, 1)   // topic_count
		payload = protocol.AppendString(payload, topic)
		payload = protocol.AppendInt32(payload, 1) // part_count
		payload = protocol.AppendInt32(payload, int32(*partition))
		payload = protocol.AppendInt32(payload, 1) // rec_count
		payload = protocol.AppendBytes(payload, keyBytes)
		payload = protocol.AppendBytes(payload, []byte(v))

		errCode, _, err := c.rpc(protocol.APIKeyProduce, payload)
		if err != nil {
			return err
		}
		if errCode != protocol.ErrNone {
			return fmt.Errorf("produce error: %v", errCode)
		}
		return nil
	}

	if *value != "" {
		// Single-value mode.
		for i := 0; i < *count; i++ {
			if err := send(*key, *value); err != nil {
				return err
			}
			fmt.Printf("→ [%s] key=%q value=%q\n", topic, *key, *value)
		}
		return nil
	}

	// Stdin mode: one message per line.
	fmt.Fprintln(os.Stderr, "Reading from stdin (Ctrl-D to stop)...")
	scanner := bufio.NewScanner(os.Stdin)
	sent := 0
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if line == "" {
			continue
		}
		if err := send(*key, line); err != nil {
			return err
		}
		sent++
		fmt.Printf("→ [%s] key=%q value=%q\n", topic, *key, line)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("stdin error: %w", err)
	}
	fmt.Printf("Produced %d messages to %q\n", sent, topic)
	return nil
}

// cmdConsume handles: mk consume [flags] <topic>
func cmdConsume(args []string, addr string) error {
	fs := newFlagSet("mk consume [--partition N] [--from-beginning] [--group G] <topic>")
	partition := fs.Int("partition", -1, "Partition to consume (-1 = all)")
	fromBeginning := fs.Bool("from-beginning", false, "Start from offset 0 (default: from latest)")
	group := fs.String("group", "", "Consumer group ID (commits offsets after each record)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: mk consume [--partition N] [--from-beginning] [--group G] <topic>")
	}
	topic := fs.Arg(0)

	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	allPartitions, err := discoverPartitions(c, topic)
	if err != nil {
		return fmt.Errorf("discover partitions for %q: %w", topic, err)
	}
	if len(allPartitions) == 0 {
		return fmt.Errorf("topic %q not found or has no partitions", topic)
	}

	var consume []int32
	if *partition >= 0 {
		consume = []int32{int32(*partition)}
	} else {
		consume = allPartitions
	}

	offsets := make(map[int32]int64, len(consume))
	for _, p := range consume {
		if *fromBeginning {
			offsets[p] = 0
		} else if *group != "" {
			committed, err := fetchCommittedOffset(c, *group, topic, p)
			if err != nil || committed < 0 {
				offsets[p] = 0
			} else {
				offsets[p] = committed + 1
			}
		} else {
			offsets[p] = 0
		}
	}

	fmt.Fprintf(os.Stderr, "Consuming %q partitions=%v — Ctrl-C to stop\n", topic, consume)

	for {
		fetched := 0
		for _, partID := range consume {
			records, err := fetchPartition(c, topic, partID, offsets[partID])
			if err != nil {
				fmt.Fprintf(os.Stderr, "fetch partition %d: %v\n", partID, err)
				continue
			}
			for _, r := range records {
				k := string(r.key)
				if k == "" {
					k = "(null)"
				}
				fmt.Printf("[partition=%-2d offset=%-6d] key=%-15q value=%s\n",
					partID, r.offset, k, string(r.value))
				offsets[partID] = r.offset + 1
				fetched++

				if *group != "" {
					commitOffset(c, *group, topic, partID, r.offset)
				}
			}
		}
		if fetched == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// ---- shared helpers --------------------------------------------------------

type record struct {
	offset int64
	key    []byte
	value  []byte
}

func discoverPartitions(c *client, topic string) ([]int32, error) {
	var payload []byte
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendString(payload, topic)

	_, body, err := c.rpc(protocol.APIKeyMetadata, payload)
	if err != nil {
		return nil, err
	}

	pos := 0
	brokerCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4
	for i := 0; i < brokerCount; i++ {
		pos += 4
		hl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2 + hl + 4
	}

	topicCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4
	if topicCount == 0 {
		return nil, nil
	}

	topicErr := int16(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2 + nl
	partCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if topicErr != 0 {
		return nil, fmt.Errorf("topic error %d", topicErr)
	}

	partitions := make([]int32, partCount)
	for p := 0; p < partCount; p++ {
		pos += 2
		partID := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4 + 4
		replicaCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4 + replicaCount*4
		isrCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4 + isrCount*4
		partitions[p] = partID
	}
	return partitions, nil
}

func fetchPartition(c *client, topic string, partID int32, fromOffset int64) ([]record, error) {
	var payload []byte
	payload = protocol.AppendInt32(payload, 500)
	payload = protocol.AppendInt32(payload, 0)
	payload = protocol.AppendInt32(payload, 1<<20)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendString(payload, topic)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendInt32(payload, partID)
	payload = protocol.AppendInt64(payload, fromOffset)
	payload = protocol.AppendInt32(payload, 1<<20)

	_, body, err := c.rpc(protocol.APIKeyFetch, payload)
	if err != nil {
		return nil, err
	}

	pos := 4
	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2 + nl
	pos += 4
	pos += 4

	partErrCode := int16(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	pos += 8

	if partErrCode != 0 {
		return nil, nil
	}

	recCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	records := make([]record, 0, recCount)
	for i := 0; i < recCount; i++ {
		offset := int64(binary.BigEndian.Uint64(body[pos:]))
		pos += 8
		pos += 8

		keyLen := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		var key []byte
		if keyLen >= 0 {
			key = make([]byte, keyLen)
			copy(key, body[pos:pos+int(keyLen)])
			pos += int(keyLen)
		}

		valLen := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		var val []byte
		if valLen >= 0 {
			val = make([]byte, valLen)
			copy(val, body[pos:pos+int(valLen)])
			pos += int(valLen)
		}

		records = append(records, record{offset: offset, key: key, value: val})
	}
	return records, nil
}

func fetchCommittedOffset(c *client, group, topic string, partID int32) (int64, error) {
	var payload []byte
	payload = protocol.AppendString(payload, group)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendString(payload, topic)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendInt32(payload, partID)

	_, body, err := c.rpc(protocol.APIKeyOffsetFetch, payload)
	if err != nil {
		return -1, err
	}

	pos := 4
	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2 + nl + 4 + 4
	committed := int64(binary.BigEndian.Uint64(body[pos:]))
	return committed, nil
}

func commitOffset(c *client, group, topic string, partID int32, offset int64) {
	var payload []byte
	payload = protocol.AppendString(payload, group)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendString(payload, topic)
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendInt32(payload, partID)
	payload = protocol.AppendInt64(payload, offset)
	payload = protocol.AppendString(payload, "")
	c.rpc(protocol.APIKeyOffsetCommit, payload) //nolint:errcheck
}
