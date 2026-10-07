package protocol

import "errors"

// OPEN_ACK status bytes (§7.1), carried in the mux OPEN_ACK frame.
const (
	AckOK             byte = 0x00 // dialed the local service, forwarding
	AckPortNotAllowed byte = 0x01 // port id not in the pushed allow-list
	AckDialFailed     byte = 0x02 // local dial failed
	AckRejected       byte = 0x03 // any other refusal (draining, bad stream, ...)
)

// ErrBadPortID means a port id fell outside 1..65535.
var ErrBadPortID = errors.New("port id out of range")

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
