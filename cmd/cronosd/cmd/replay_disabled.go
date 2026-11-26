//go:build !replay
// +build !replay

package cmd

import (
	"errors"
	"io"
)

var errReplayDisabled = errors.New("block replay needs a build with the replay tag")

func RunReplayWorker(string, string, io.Reader, io.Writer) error { return errReplayDisabled }

func RunReplayServer(string, string) error { return errReplayDisabled }
