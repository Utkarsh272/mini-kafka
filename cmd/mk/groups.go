package main

import (
	"encoding/binary"
	"fmt"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

func cmdGroups(args []string, addr string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mk groups <describe> [flags]")
	}
	switch args[0] {
	case "describe":
		return groupsDescribe(args[1:], addr)
	default:
		return fmt.Errorf("unknown groups subcommand %q (describe)", args[0])
	}
}

// groupsDescribe shows the state of a consumer group.
//
// If the group exists in the coordinator (has active members), it shows full
// state including member assignments. If the group only has committed offsets
// (consumers have disconnected), it falls back to showing the offset store data.
func groupsDescribe(args []string, addr string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mk groups describe <group-id>")
	}
	groupID := args[0]

	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.close()

	// Try DescribeGroup first — works when consumers are actively connected.
	var payload []byte
	payload = protocol.AppendString(payload, groupID)

	errCode, body, err := c.rpc(protocol.APIKeyDescribeGroup, payload)
	if err != nil {
		return err
	}
	if errCode != protocol.ErrNone {
		return fmt.Errorf("describe group error: %v", errCode)
	}

	pos := 0
	innerErr := int16(binary.BigEndian.Uint16(body[pos:]))
	pos += 2

	if innerErr == 0 {
		// Group found in coordinator — print full state.
		return printGroupState(body[2:], groupID)
	}

	// Group not in coordinator (innerErr=25 = unknown member).
	// This is normal: the CLI's consume --group only commits offsets via
	// OffsetCommit without doing JoinGroup. Show offset summary instead.
	fmt.Printf("Group:  %s\n", groupID)
	fmt.Printf("State:  Inactive (no active members — showing committed offsets)\n\n")

	// Discover all topics to scan offsets.
	var metaPayload []byte
	metaPayload = protocol.AppendInt32(metaPayload, 0) // all topics
	_, metaBody, err := c.rpc(protocol.APIKeyMetadata, metaPayload)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	topics, partMap := parseMetadataTopics(metaBody)
	if len(topics) == 0 {
		fmt.Println("  (no topics to show offsets for)")
		return nil
	}

	fmt.Printf("  %-25s  %-10s  %-15s\n", "TOPIC", "PARTITION", "COMMITTED OFFSET")
	fmt.Printf("  %-25s  %-10s  %-15s\n", "-----", "---------", "----------------")

	for _, topic := range topics {
		for _, partID := range partMap[topic] {
			committed, err := fetchCommittedOffset(c, groupID, topic, partID)
			if err != nil || committed < 0 {
				continue // no committed offset for this partition
			}
			fmt.Printf("  %-25s  %-10d  %d\n", topic, partID, committed)
		}
	}
	return nil
}

// printGroupState decodes and prints a full DescribeGroup response body
// (after the outer error_code has already been consumed).
func printGroupState(body []byte, groupID string) error {
	pos := 0

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

	fmt.Printf("Group:      %s\n", gid)
	fmt.Printf("State:      %s\n", state)
	fmt.Printf("Generation: %d\n", genID)
	fmt.Printf("Leader:     %s\n", leaderID)
	fmt.Printf("Members:    %d\n", memberCount)

	if memberCount == 0 {
		fmt.Println("  (no active members)")
		return nil
	}

	fmt.Println()
	fmt.Printf("  %-36s  %-15s  %s\n", "MEMBER-ID", "CLIENT-ID", "ASSIGNMENT")
	fmt.Printf("  %-36s  %-15s  %s\n", "---------", "---------", "----------")

	for i := 0; i < memberCount; i++ {
		ml := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		memberID := string(body[pos : pos+ml])
		pos += ml

		cl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		clientID := string(body[pos : pos+cl])
		pos += cl

		topicCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4

		assignment := ""
		for t := 0; t < topicCount; t++ {
			tl := int(binary.BigEndian.Uint16(body[pos:]))
			pos += 2
			topicName := string(body[pos : pos+tl])
			pos += tl
			partCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4
			assignment += topicName + ":"
			for p := 0; p < partCount; p++ {
				partID := int32(binary.BigEndian.Uint32(body[pos:]))
				pos += 4
				if p > 0 {
					assignment += ","
				}
				assignment += fmt.Sprintf("%d", partID)
			}
			if t < topicCount-1 {
				assignment += " "
			}
		}
		if assignment == "" {
			assignment = "(none)"
		}

		displayID := memberID
		if len(displayID) > 36 {
			displayID = displayID[:33] + "..."
		}
		fmt.Printf("  %-36s  %-15s  %s\n", displayID, clientID, assignment)
	}
	return nil
}

// parseMetadataTopics parses a Metadata response body and returns
// a sorted topic list and a map of topic → []partitionID.
func parseMetadataTopics(body []byte) ([]string, map[string][]int32) {
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

	topics := make([]string, 0, topicCount)
	partMap := make(map[string][]int32)

	for i := 0; i < topicCount; i++ {
		pos += 2 // topic errCode
		nl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		name := string(body[pos : pos+nl])
		pos += nl
		partCount := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4

		parts := make([]int32, partCount)
		for p := 0; p < partCount; p++ {
			pos += 2
			partID := int32(binary.BigEndian.Uint32(body[pos:]))
			pos += 4 + 4
			replicaCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4 + replicaCount*4
			isrCount := int(binary.BigEndian.Uint32(body[pos:]))
			pos += 4 + isrCount*4
			parts[p] = partID
		}

		topics = append(topics, name)
		partMap[name] = parts
	}
	return topics, partMap
}
