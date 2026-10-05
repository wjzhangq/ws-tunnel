package wsutil

import (
	"testing"

	"ws-tunnel/internal/mux"
)

func TestReadLimitExceedsMuxPayload(t *testing.T) {
	if mux.MaxPayload >= ReadLimit {
		t.Fatalf("mux.MaxPayload %d must stay below ReadLimit %d", mux.MaxPayload, ReadLimit)
	}
}
