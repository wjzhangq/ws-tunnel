package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Stream header / ack constants (§7.1).
//
//	server → client:  ver(1B) | portID(varint) | raw L4 bytes...
//	client → server:  status(1B) | raw L4 bytes...
const (
	StreamVersion byte = 0x01

	AckOK             byte = 0x00 // dialed the local service, forwarding
	AckPortNotAllowed byte = 0x01 // port id not in the pushed allow-list
	AckDialFailed     byte = 0x02 // local dial failed
	AckRejected       byte = 0x03 // any other refusal (draining, bad header, ...)
)

var (
	// ErrBadVersion means the header's ver byte is not StreamVersion; the
	// client closes the stream immediately (§7.1).
	ErrBadVersion = errors.New("unsupported stream header version")
	// ErrBadPortID means the varint port id fell outside 1..65535.
	ErrBadPortID = errors.New("stream header port id out of range")
)

// AckResult maps an ack byte to the `result` label used by
// tunnel_stream_open_total (§13.2).
func AckResult(status byte) string {
	switch status {
	case AckOK:
		return "ok"
	case AckPortNotAllowed:
		return "not_allowed"
	case AckDialFailed:
		return "dial_failed"
	case AckRejected:
		return "rejected"
	default:
		return "rejected"
	}
}

// AckReason returns a human-readable explanation for logs and /status.
func AckReason(status byte) string {
	switch status {
	case AckOK:
		return "ok"
	case AckPortNotAllowed:
		return "port id not in allow-list"
	case AckDialFailed:
		return "client failed to dial the local service"
	case AckRejected:
		return "client refused the stream"
	default:
		return "unknown ack status"
	}
}

// WriteStreamHeader writes ver + varint(portID) in a single Write so it lands
// in one smux frame.
func WriteStreamHeader(w io.Writer, portID int) error {
	buf := make([]byte, 1, 1+binary.MaxVarintLen32)
	buf[0] = StreamVersion
	buf = binary.AppendUvarint(buf, uint64(portID))
	_, err := w.Write(buf)
	return err
}

// ReadStreamHeader reads the header from a stream. It reads exactly as many
// bytes as the header occupies so the payload that follows is left untouched.
func ReadStreamHeader(r io.Reader) (int, error) {
	br := &byteReader{r: r}
	ver, err := br.ReadByte()
	if err != nil {
		return 0, err
	}
	if ver != StreamVersion {
		return 0, ErrBadVersion
	}
	id, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, err
	}
	if id < 1 || id > 65535 {
		return 0, ErrBadPortID
	}
	return int(id), nil
}

// WriteAck writes the single ack byte.
func WriteAck(w io.Writer, status byte) error {
	_, err := w.Write([]byte{status})
	return err
}

// ReadAck reads the single ack byte.
func ReadAck(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// byteReader adapts an io.Reader to io.ByteReader without buffering ahead,
// which matters because everything after the header is opaque payload.
type byteReader struct {
	r   io.Reader
	buf [1]byte
}

func (b *byteReader) ReadByte() (byte, error) {
	if _, err := io.ReadFull(b.r, b.buf[:]); err != nil {
		return 0, err
	}
	return b.buf[0], nil
}
