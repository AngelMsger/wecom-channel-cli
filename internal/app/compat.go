package app

import (
	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/spf13/cobra"
)

// Published paths remain callable, but discovery advertises one canonical path.
// The same command factory owns both routes; aliases cannot change authority.
func compatibilityCommand(cmd *cobra.Command, replacement string) *cobra.Command {
	cmd.Hidden = true
	cmd.Short = "Compatibility alias for " + replacement
	cmd.Long = "This published command remains available for existing scripts. Use " + replacement + " for new integrations."
	run := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"deprecated_command": cmd.CommandPath(), "replacement": "wecom-channel-cli " + replacement}})
		return run(cmd, args)
	}
	return cmd
}
