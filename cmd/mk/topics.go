package main

import (
	"encoding/binary"
	"fmt"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

func cmdTopics(args []string, addr string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mk topics <list|create|describe> [flags]")
	}
	switch args[0] {
	case "list":
		return topicsList(addr)
	case "create":
		return topicsCreate(args[1:], addr)
	case "describe":
		return topicsDescribe(args[1:], addr)
	default:
		return fmt.Errorf("unknown topics subcommand %q (list|create|describe)", args[0])
	}
}

func topicsList(addr string) error {
	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	var payload []byte
	payload = protocol.AppendInt32(payload, 0)

	errCode, body, err := c.rpc(protocol.APIKeyMetadata, payload)
	if err != nil {
		return err
	}
	if errCode != protocol.ErrNone {
		return fmt.Errorf("metadata error: %v", errCode)
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
		fmt.Println("(no topics)")
		return nil
	}

	fmt.Printf("%-30s  %s\n", "TOPIC", "PARTITIONS")
	fmt.Printf("%-30s  %s\n", "-----", "----------")

	for i := 0; i < topicCount; i++ {
		topicErr := int16(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		nl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		name := string(body[pos : pos+nl])
		pos += nl
		partCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		for p := 0; p < partCount; p++ {
			pos += 2 + 4 + 4
			replicaCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4 + replicaCount*4
			isrCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4 + isrCount*4
		}
		if topicErr != 0 {
			fmt.Printf("%-30s  (error %d)\n", name, topicErr)
		} else {
			fmt.Printf("%-30s  %d\n", name, partCount)
		}
	}
	return nil
}

// topicsCreate: flags come BEFORE the topic name.
// Usage: mk topics create [--partitions N] [--replication-factor N] <topic>
func topicsCreate(args []string, addr string) error {
	fs := newFlagSet("mk topics create [--partitions N] [--replication-factor N] <topic>")
	partitions := fs.Int("partitions", 1, "Number of partitions")
	rf := fs.Int("replication-factor", 1, "Replication factor")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: mk topics create [--partitions N] [--replication-factor N] <topic>")
	}
	topic := fs.Arg(0)

	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	var payload []byte
	payload = protocol.AppendString(payload, topic)
	payload = protocol.AppendInt32(payload, int32(*partitions))
	payload = protocol.AppendInt32(payload, int32(*rf))

	errCode, body, err := c.rpc(protocol.APIKeyCreateTopic, payload)
	if err != nil {
		return err
	}
	if errCode != protocol.ErrNone {
		return fmt.Errorf("create topic error: %v", errCode)
	}

	nl := int(binary.BigEndian.Uint16(body[0:]))
	innerErr := int16(binary.BigEndian.Uint16(body[2+nl:]))
	if innerErr != 0 {
		if innerErr == 36 {
			return fmt.Errorf("topic %q already exists", topic)
		}
		return fmt.Errorf("create topic inner error: %d", innerErr)
	}

	fmt.Printf("Created topic %q (%d partitions, RF=%d)\n", topic, *partitions, *rf)
	return nil
}

func topicsDescribe(args []string, addr string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mk topics describe <topic>")
	}
	topic := args[0]

	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	var payload []byte
	payload = protocol.AppendInt32(payload, 1)
	payload = protocol.AppendString(payload, topic)

	errCode, body, err := c.rpc(protocol.APIKeyMetadata, payload)
	if err != nil {
		return err
	}
	if errCode != protocol.ErrNone {
		return fmt.Errorf("metadata error: %v", errCode)
	}

	pos := 0
	brokerCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	type brokerInfo struct {
		nodeID int32
		host   string
		port   int32
	}
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
		brokers[i] = brokerInfo{nodeID, host, port}
	}

	topicCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4
	if topicCount == 0 {
		return fmt.Errorf("topic %q not found", topic)
	}

	topicErr := int16(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	nl := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	name := string(body[pos : pos+nl])
	pos += nl
	partCount := int(binary.BigEndian.Uint32(body[pos:]))
	pos += 4

	if topicErr != 0 {
		return fmt.Errorf("topic %q not found (error %d)", topic, topicErr)
	}

	fmt.Printf("Topic: %s\n", name)
	fmt.Printf("  Partitions: %d\n\n", partCount)
	fmt.Printf("  %-10s  %-20s  %s\n", "PARTITION", "LEADER", "REPLICAS")
	fmt.Printf("  %-10s  %-20s  %s\n", "---------", "------", "-------")

	for p := 0; p < partCount; p++ {
		pos += 2
		partID := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		leaderID := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += 4

		replicaCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		replicas := make([]int32, replicaCount)
		for i := 0; i < replicaCount; i++ {
			replicas[i] = int32(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
		}
		isrCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		pos += isrCount * 4

		leaderHost := fmt.Sprintf("node-%d", leaderID)
		for _, b := range brokers {
			if b.nodeID == leaderID {
				leaderHost = fmt.Sprintf("%s:%d", b.host, b.port)
				break
			}
		}

		replicaStr := ""
		for i, r := range replicas {
			if i > 0 {
				replicaStr += ","
			}
			replicaStr += fmt.Sprintf("%d", r)
		}
		fmt.Printf("  %-10d  %-20s  [%s]\n", partID, leaderHost, replicaStr)
	}
	return nil
}
