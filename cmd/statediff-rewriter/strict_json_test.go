package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStrictJSONRejectsUnknownAndTrailingFields(t *testing.T) {
	type document struct {
		Schema string `json:"schema"`
	}
	var decoded document
	require.NoError(t, decodeStrictJSON([]byte(`{"schema":"v1"}`), &decoded, "document"))
	require.Equal(t, "v1", decoded.Schema)

	require.ErrorContains(
		t, decodeStrictJSON([]byte(`{"schema":"v1","extra":true}`), &decoded, "document"), "unknown field",
	)
	require.ErrorContains(
		t, decodeStrictJSON([]byte(`{"schema":"v1"} {"schema":"v2"}`), &decoded, "document"), "trailing JSON",
	)
}
