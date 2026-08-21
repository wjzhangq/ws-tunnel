package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// Payload framing (§7.1).
//
// smux offers no half-close: Stream.Close both emits cmdFIN and tears down the
// local read side, so a peer that closes its stream to relay a TCP FIN loses
// the reply that is still in flight. The payload therefore carries its own
// end-of-direction marker: the writer sends frameEOF and keeps the stream open
// to read the answer, and the reader surfaces that marker as io.EOF.
//
//	frameData | len(varint) | len bytes
//	frameEOF                              (no more bytes in this direction)
const (
	frameData byte = 0x00
	frameEOF  byte = 0x01

	// maxFrameLen bounds a single frame so a hostile or desynchronised peer
	// cannot make us allocate without limit. io.Copy uses 32 KiB buffers, so
	// this leaves ample headroom.
	maxFrameLen = 1 << 20
)

// ErrBadFrame means the framed payload is malformed: an unknown frame type, an
// oversized length, or bytes arriving after frameEOF.
var ErrBadFrame = errors.New("malformed stream payload frame")

// FrameWriter wraps each Write in a data frame and turns CloseWrite into an
// explicit end-of-direction marker. It is safe for one writing goroutine; a
// mutex guards against CloseWrite racing that goroutine's final Write.
type FrameWriter struct {
	w   io.Writer
	mu  sync.Mutex
	hdr [1 + binary.MaxVarintLen32]byte
	buf []byte
	eof bool
}

// NewFrameWriter frames everything written to w.
func NewFrameWriter(w io.Writer) *FrameWriter { return &FrameWriter{w: w} }

func (f *FrameWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.eof {
		return 0, io.ErrClosedPipe
	}
	// Header and payload go out in one Write so they land in a single smux
	// frame; splitting them would double the frame count on every copy.
	n := 1
	f.hdr[0] = frameData
	n += binary.PutUvarint(f.hdr[1:], uint64(len(p)))
	f.buf = append(f.buf[:0], f.hdr[:n]...)
	f.buf = append(f.buf, p...)
	if _, err := f.w.Write(f.buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

// CloseWrite tells the peer no further bytes will arrive in this direction,
// the framed equivalent of a TCP FIN. It leaves the stream readable. Repeated
// calls are no-ops.
func (f *FrameWriter) CloseWrite() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.eof {
		return nil
	}
	f.eof = true
	_, err := f.w.Write([]byte{frameEOF})
	return err
}

// FrameReader decodes a framed payload, reporting frameEOF as io.EOF. Only one
// goroutine may read at a time, matching io.Copy's usage.
type FrameReader struct {
	r      io.Reader
	br     byteReader
	remain int
	eof    bool
}

// NewFrameReader decodes the framed payload arriving on r.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{r: r, br: byteReader{r: r}}
}

func (f *FrameReader) Read(p []byte) (int, error) {
	if f.eof {
		return 0, io.EOF
	}
	for f.remain == 0 {
		typ, err := f.br.ReadByte()
		if err != nil {
			return 0, err
		}
		switch typ {
		case frameEOF:
			f.eof = true
			return 0, io.EOF
		case frameData:
		default:
			return 0, ErrBadFrame
		}
		n, err := binary.ReadUvarint(&f.br)
		if err != nil {
			return 0, err
		}
		if n == 0 || n > maxFrameLen {
			return 0, ErrBadFrame
		}
		f.remain = int(n)
	}
	if len(p) > f.remain {
		p = p[:f.remain]
	}
	n, err := f.r.Read(p)
	f.remain -= n
	if err == io.EOF && f.remain > 0 {
		// The stream died mid-frame; do not present a truncated frame as a
		// clean end of data.
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
