package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func decodeStrictJSON(body []byte, target any, label string) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%s has trailing JSON", label)
		}
		return fmt.Errorf("decode trailing %s: %w", label, err)
	}
	return nil
}
