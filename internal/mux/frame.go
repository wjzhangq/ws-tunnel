// Package mux multiplexes control JSON and L4 streams on one WebSocket.
//
// Text frames carry protocol.Message. Binary frames carry a compact stream
// header so a single connection can open, ack, carry, half-close and reset
// many TCP forwards without smux or a second WS.
package mux

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Version is the binary frame version. Unknown versions are a hard error.
const Version byte = 1

// Binary frame types.
const (
	TypeOpen    byte = 1
	TypeOpenAck byte = 2
	TypeData    byte = 3
	TypeFin     byte = 4
	TypeRst     byte = 5
	TypeWindow  byte = 6
)

const (
	// MaxPayload is the largest DATA body. All streams share one WebSocket
	// writer, so this bounds how long a bulk stream holds it before a small
	// frame from another stream gets a turn. It also stays well under the WS
	// read limit (1 MiB) so a peer cannot force huge allocations per frame.
	MaxPayload = 16 * 1024
	// InitialWindow is the credit an opener may spend before OPEN_ACK tells it
	// the acceptor's real receive window. It is part of the wire contract.
	InitialWindow = 64 * 1024
	// DefaultWindow is the receive window this side advertises per stream.
	// Per-stream throughput is capped at roughly window / RTT.
	DefaultWindow = 256 * 1024
	// MinWindow and MaxWindow bound WithWindow.
	MinWindow = 4 * 1024
	MaxWindow = 16 * 1024 * 1024
)

var (
	ErrBadFrame   = errors.New("malformed mux frame")
	ErrBadVersion = errors.New("unsupported mux frame version")
)

// Frame is one decoded binary message.
type Frame struct {
	Type     byte
	StreamID uint32
	Port     uint16
	Status   byte
	Window   uint32
	Reason   byte
	Payload  []byte
}

func encodeFrame(f Frame) ([]byte, error) {
	switch f.Type {
	case TypeOpen:
		b := make([]byte, 1+1+4+2+4)
		b[0] = Version
		b[1] = TypeOpen
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		binary.BigEndian.PutUint16(b[6:8], f.Port)
		binary.BigEndian.PutUint32(b[8:12], f.Window)
		return b, nil
	case TypeOpenAck:
		b := make([]byte, 1+1+4+1+4)
		b[0] = Version
		b[1] = TypeOpenAck
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		b[6] = f.Status
		binary.BigEndian.PutUint32(b[7:11], f.Window)
		return b, nil
	case TypeData:
		if len(f.Payload) == 0 {
			return nil, nil
		}
		if len(f.Payload) > MaxPayload {
			return nil, fmt.Errorf("%w: data payload %d > %d", ErrBadFrame, len(f.Payload), MaxPayload)
		}
		b := make([]byte, 1+1+4+2+len(f.Payload))
		b[0] = Version
		b[1] = TypeData
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		binary.BigEndian.PutUint16(b[6:8], uint16(len(f.Payload)))
		copy(b[8:], f.Payload)
		return b, nil
	case TypeFin:
		b := make([]byte, 1+1+4)
		b[0] = Version
		b[1] = TypeFin
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		return b, nil
	case TypeRst:
		b := make([]byte, 1+1+4+1)
		b[0] = Version
		b[1] = TypeRst
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		b[6] = f.Reason
		return b, nil
	case TypeWindow:
		b := make([]byte, 1+1+4+4)
		b[0] = Version
		b[1] = TypeWindow
		binary.BigEndian.PutUint32(b[2:6], f.StreamID)
		binary.BigEndian.PutUint32(b[6:10], f.Window)
		return b, nil
	default:
		return nil, fmt.Errorf("%w: unknown type %d", ErrBadFrame, f.Type)
	}
}

func decodeFrame(b []byte) (Frame, error) {
	if len(b) < 2 {
		return Frame{}, ErrBadFrame
	}
	if b[0] != Version {
		return Frame{}, ErrBadVersion
	}
	f := Frame{Type: b[1]}
	if len(b) < 6 && f.Type != 0 {
		return Frame{}, ErrBadFrame
	}
	switch f.Type {
	case TypeOpen:
		if len(b) != 12 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
		f.Port = binary.BigEndian.Uint16(b[6:8])
		f.Window = binary.BigEndian.Uint32(b[8:12])
	case TypeOpenAck:
		if len(b) != 11 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
		f.Status = b[6]
		f.Window = binary.BigEndian.Uint32(b[7:11])
	case TypeData:
		if len(b) < 8 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
		n := int(binary.BigEndian.Uint16(b[6:8]))
		if n == 0 || n > MaxPayload || 8+n != len(b) {
			return Frame{}, ErrBadFrame
		}
		f.Payload = b[8:]
	case TypeFin:
		if len(b) != 6 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
	case TypeRst:
		if len(b) != 7 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
		f.Reason = b[6]
	case TypeWindow:
		if len(b) != 10 {
			return Frame{}, ErrBadFrame
		}
		f.StreamID = binary.BigEndian.Uint32(b[2:6])
		f.Window = binary.BigEndian.Uint32(b[6:10])
	default:
		return Frame{}, fmt.Errorf("%w: unknown type %d", ErrBadFrame, f.Type)
	}
	return f, nil
}
