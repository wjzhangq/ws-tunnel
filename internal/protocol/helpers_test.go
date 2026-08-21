package protocol

import "encoding/json"

func unmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }
func marshal(v any) ([]byte, error)   { return json.Marshal(v) }
