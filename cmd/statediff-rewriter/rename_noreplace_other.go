//go:build !darwin && !linux

package main

import "fmt"

func renameNoReplace(_, _ string) error {
	return fmt.Errorf("atomic no-replace rename is unsupported on this platform")
}
