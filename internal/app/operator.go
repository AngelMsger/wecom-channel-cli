package app

import (
	"os"

	"github.com/angelmsger/agent-cli-kit/output"
	"github.com/angelmsger/wecom-channel-cli/internal/broker"
	"github.com/angelmsger/wecom-channel-cli/internal/config"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	"github.com/angelmsger/wecom-channel-cli/pkg/callback"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/spf13/cobra"
)

func (s *appState) brokerCommands() *cobra.Command {
	g := &cobra.Command{Use: "broker", Short: "Run the isolated credential-holding broker"}
	var path string
	var dry bool
	c := &cobra.Command{Use: "serve", Short: "Serve agent/operator Unix sockets and optional encrypted callbacks", Args: cobra.NoArgs}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		deployment, err := config.LoadDeployment(path)
		if err != nil {
			return err
		}
		if dry {
			return s.emit(cmd, map[string]any{"dry_run": true, "policy_digest": apiclient.Digest(deployment), "policy": deployment, "next_steps": []string{"Provision isolated OS accounts and socket directories as described in docs/security.md."}})
		}
		if err := s.guard(); err != nil {
			return err
		}
		api, err := apiclient.New(deployment.Bot, os.Getenv("WECOM_CHANNEL_BOT_SECRET"))
		if err != nil {
			return err
		}
		var verifier *callback.Verifier
		if deployment.CallbackListen != "" {
			verifier, err = callback.New(os.Getenv("WECOM_CHANNEL_CALLBACK_TOKEN"), os.Getenv("WECOM_CHANNEL_ENCODING_AES_KEY"), "")
			if err != nil {
				return e.New(e.CategoryAuth, "CALLBACK_CREDENTIALS", "Callback Token and a valid 43-character EncodingAESKey must be supplied to the broker.")
			}
		}
		var brokers []*broker.Broker
		for _, cfg := range deployment.Contexts {
			scope := ""
			if deployment.Version == 2 {
				scope = cfg.Name + "\x00" + cfg.Channels[0].ID
			}
			st, err := state.Open(cfg.StateDir, cfg.Bot, scope)
			if err != nil {
				return err
			}
			defer st.Close()
			brokers = append(brokers, broker.New(cfg, st, api, verifier))
		}
		return broker.ServeContexts(cmd.Context(), brokers, deployment.CallbackListen, func() {
			output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"broker": "ready", "contexts": len(brokers), "callback_enabled": deployment.CallbackListen != ""}})
		})
	}
	c.Flags().StringVar(&path, "config", "", "Absolute operator-owned policy file")
	c.Flags().BoolVar(&dry, "dry-run", false, "Validate and preview without credentials, sockets or state writes")
	g.AddCommand(c)
	return g
}

func ExampleConfig() config.Config {
	c := config.Default()
	c.Bot = "BOT_ID"
	c.StateDir = "/var/lib/wecom-channel"
	c.AgentSocket = "/run/wecom-channel-agent/agent.sock"
	c.AdminSocket = "/run/wecom-channel-admin/admin.sock"
	c.Events = []string{"topic_event", "topic_reply_event"}
	c.Channels = []config.Channel{{ID: "CHANNEL_ID", ReadAll: true, ObserveAll: true}}
	return c
}

func (s *appState) configCommands() *cobra.Command {
	g := &cobra.Command{Use: "config", Short: "Inspect operator policy offline"}
	var path string
	g.AddCommand(&cobra.Command{Use: "example", Short: "Print a deny-by-default operator policy template", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return s.emit(cmd, ExampleConfig()) }})
	c := &cobra.Command{Use: "validate", Short: "Strictly validate an operator policy file without credentials or I/O effects", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		deployment, err := config.LoadDeployment(path)
		if err != nil {
			return err
		}
		return s.emit(cmd, map[string]any{"valid": true, "policy_digest": apiclient.Digest(deployment), "contexts": len(deployment.Contexts)})
	}}
	c.Flags().StringVar(&path, "file", "", "Operator policy file")
	g.AddCommand(c)
	s.addContextCommands(g)
	return g
}

func (s *appState) authCommands() *cobra.Command {
	g := &cobra.Command{Use: "auth", Short: "Explain credential ownership and verify through the broker"}
	g.AddCommand(&cobra.Command{Use: "guide", Short: "Read offline authentication and official CLI compatibility guidance", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		return s.emit(cmd, map[string]any{
			"scheme": "wecom-aibot-sha256", "endpoint": apiclient.Origin + "/cgi-bin/aibot/get_token",
			"credential_owner": "isolated broker operator", "required_broker_environment": []string{"WECOM_CHANNEL_BOT_SECRET"},
			"callback_environment": []string{"WECOM_CHANNEL_CALLBACK_TOKEN", "WECOM_CHANNEL_ENCODING_AES_KEY"},
			"watch_setup":          "Polling needs read_all and observe_all with post/reply events enabled; leave callback_listen empty. Callback environment variables are optional and required only when a callback listener is configured.",
			"credential_guide":     "Enable API mode in the WeCom bot administration page. Supply the existing bot ID and Secret to the broker's secret manager; do not send them through chat or agent arguments.",
			"official_cli_reuse":   map[string]any{"supported": false, "reason": "The official CLI uses get_cli_config and a gateway Bearer token. Channel get_token credentials are a separate contract; encrypted private stores are not an integration API."},
			"documentation":        "https://developer.work.weixin.qq.com/document/path/101910",
			"next_steps":           []string{"wecom-channel-cli config example", "wecom-channel-cli broker serve --help", "wecom-channel-cli auth check --help"},
		})
	}})
	g.AddCommand(&cobra.Command{Use: "status", Short: "Read broker readiness (does not verify WeCom authentication)", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return s.call(cmd, broker.Request{Operation: "health"}) }})
	g.AddCommand(&cobra.Command{Use: "check", Short: "Operator-only: verify authentication with a bounded channel-list request", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		return s.call(cmd, broker.Request{Operation: "auth.check"})
	}})
	return g
}
