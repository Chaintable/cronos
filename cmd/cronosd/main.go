package main

import (
	"fmt"
	"os"

	svrcmd "github.com/cosmos/cosmos-sdk/server/cmd"
	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/cmd"
)

func main() {
	if len(os.Args) == 4 && os.Args[1] == "replay-worker" {
		exit(cmd.RunReplayWorker(os.Args[2], os.Args[3], os.Stdin, os.Stderr))
	}
	if len(os.Args) == 4 && os.Args[1] == "replay-serve" {
		exit(cmd.RunReplayServer(os.Args[2], os.Args[3]))
	}
	rootCmd, _ := cmd.NewRootCmd()
	if err := svrcmd.Execute(rootCmd, cmd.EnvPrefix, app.DefaultNodeHome); err != nil {
		os.Exit(1)
	}
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
