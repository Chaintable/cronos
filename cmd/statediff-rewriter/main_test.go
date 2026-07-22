package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandsRejectPositionalArguments(t *testing.T) {
	root := newRootCommand()
	commands := append([]string{root.Name()}, "dump", "plan", "apply", "verify", "rollback")
	for _, name := range commands {
		command := root
		if name != root.Name() {
			found, _, err := root.Find([]string{name})
			require.NoError(t, err)
			command = found
		}
		require.Error(t, command.Args(command, []string{"unexpected"}), name)
	}
}

func TestRootVersionOutput(t *testing.T) {
	var output bytes.Buffer
	root := newRootCommand()
	root.SetOut(&output)
	root.SetArgs([]string{"--version"})
	require.NoError(t, root.Execute())
	require.Equal(t, buildCommit()+"\n", output.String())
}

func TestCheckpointIntervalOnlyAppearsOnTimeBasedCheckpointCommands(t *testing.T) {
	root := newRootCommand()
	for _, name := range []string{"plan", "verify"} {
		command, _, err := root.Find([]string{name})
		require.NoError(t, err)
		require.NotNil(t, command.Flags().Lookup("checkpoint-interval"), name)
	}
	for _, name := range []string{"apply", "rollback"} {
		command, _, err := root.Find([]string{name})
		require.NoError(t, err)
		require.Nil(t, command.Flags().Lookup("checkpoint-interval"), name)
	}
}
