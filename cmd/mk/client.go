package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/Utkarsh272/mini-kafka/internal/protocol"
)

// client is a minimal wire-protocol client for the mk CLI.
// It maintains a single TCP connection to a broker.
type client struct {
	conn   net.Conn
	br     *bufio.Reader
	bw     *bufio.Writer
	corrID uint32
}

// dial opens a TCP connection to addr.
func dial(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return &client{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 256*1024),
		bw:   bufio.NewWriterSize(conn, 256*1024),
	}, nil
}

func (c *client) close() {
	c.conn.Close()
}

// send encodes and transmits one request frame.
// Returns the correlation ID used.
func (c *client) send(apiKey protocol.APIKey, payload []byte) uint32 {
	c.corrID++
	id := c.corrID

	clientID := []byte("mk")
	bodyLen := 1 + 4 + 2 + len(clientID) + len(payload)
	frame := make([]byte, 4+bodyLen)

	binary.BigEndian.PutUint32(frame[0:], uint32(bodyLen))
	frame[4] = byte(apiKey)
	binary.BigEndian.PutUint32(frame[5:], id)
	binary.BigEndian.PutUint16(frame[9:], uint16(len(clientID)))
	copy(frame[11:], clientID)
	copy(frame[11+len(clientID):], payload)

	c.bw.Write(frame)
	c.bw.Flush()
	return id
}

// recv reads one complete response frame and returns
// (correlationID, outerErrorCode, payload).
func (c *client) recv() (uint32, protocol.ErrorCode, []byte, error) {
	var totalLen uint32
	if err := binary.Read(c.br, binary.BigEndian, &totalLen); err != nil {
		return 0, 0, nil, fmt.Errorf("read length: %w", err)
	}
	body := make([]byte, totalLen)
	if _, err := io.ReadFull(c.br, body); err != nil {
		return 0, 0, nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) < 6 {
		return 0, 0, nil, fmt.Errorf("response too short (%d bytes)", len(body))
	}
	corrID := binary.BigEndian.Uint32(body[0:])
	errCode := protocol.ErrorCode(int16(binary.BigEndian.Uint16(body[4:])))
	return corrID, errCode, body[6:], nil
}

// rpc is a convenience wrapper: send + recv in one call.
func (c *client) rpc(apiKey protocol.APIKey, payload []byte) (protocol.ErrorCode, []byte, error) {
	c.send(apiKey, payload)
	_, errCode, body, err := c.recv()
	return errCode, body, err
}
