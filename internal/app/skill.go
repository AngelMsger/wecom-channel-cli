package app

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/angelmsger/agent-cli-kit/output"
	kit "github.com/angelmsger/agent-cli-kit/skill"
	"github.com/angelmsger/wecom-channel-cli/internal/state"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/skills"
	"github.com/spf13/cobra"
)

func skillDriftNotice(cmd *cobra.Command) {
	if os.Getenv("WECOM_CHANNEL_NO_SKILL_HINT") == "1" {
		return
	}
	data, _ := skills.Files.ReadFile("wecom-channel/SKILL.md")
	version := kit.Version(data)
	loaded := kit.Loaded(os.Getenv("WECOM_CHANNEL_SKILL_LOADED"), version)
	if loaded.Loaded && loaded.Status != "current" {
		output.EmitNotice(cmd.ErrOrStderr(), map[string]any{"_notice": map[string]any{"skill": map[string]any{"loaded_version": loaded.Version, "embedded_version": version, "status": loaded.Status}, "next_steps": []string{"wecom-channel-cli skill install", "Reload the agent context to load the matching Skill."}, "silence": "WECOM_CHANNEL_NO_SKILL_HINT=1"}})
	}
}

func (s *appState) skillCommands() *cobra.Command {
	g := &cobra.Command{Use: "skill", Short: "Inspect or install the embedded companion Skill"}
	data, _ := skills.Files.ReadFile("wecom-channel/SKILL.md")
	version := kit.Version(data)
	g.AddCommand(&cobra.Command{Use: "show", Short: "Print the embedded Skill", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { _, err := cmd.OutOrStdout().Write(data); return err }})
	for _, verb := range []string{"status", "path", "install"} {
		var agent, dir string
		var project, dry, overwrite bool
		c := &cobra.Command{Use: verb, Short: verb + " companion Skill for detected or selected agent hosts", Args: cobra.NoArgs}
		c.RunE = func(cmd *cobra.Command, _ []string) error {
			paths, err := skillPaths(agent, dir, project)
			if err != nil {
				return err
			}
			rows := []map[string]any{}
			for _, path := range paths {
				observed := kit.Inspect(os.DirFS(filepath.Dir(path)), filepath.Base(path)+"/SKILL.md", version)
				row := map[string]any{"path": path, "status": observed.Status, "version": observed.Version, "alignment": observed.Alignment}
				if verb == "install" {
					if !dry {
						if err := s.guard(); err != nil {
							return err
						}
					}
					if err := installSkill(path, data, dry, overwrite); err != nil {
						return err
					}
					row["dry_run"] = dry
					row["version"] = version
					row["status"] = "installed"
					if dry {
						row["status"] = "planned"
					}
				}
				rows = append(rows, row)
			}
			if verb == "status" {
				loaded := kit.Loaded(os.Getenv("WECOM_CHANNEL_SKILL_LOADED"), version)
				return s.emit(cmd, map[string]any{"loaded": loaded.Loaded, "loaded_version": loaded.Version, "loaded_status": loaded.Status, "embedded_version": version, "installs": rows, "next_steps": []string{"wecom-channel-cli skill install", "Reload the agent context after a CLI or Skill upgrade."}})
			}
			return s.emit(cmd, rows)
		}
		c.Flags().StringVar(&agent, "agent", "", "Agent ID supported by agent-cli-kit (for example codex or claude)")
		c.Flags().StringVar(&dir, "dir", "", "Explicit parent skills directory")
		c.Flags().BoolVar(&project, "project", false, "Use project rather than global agent paths")
		if verb == "install" {
			c.Flags().BoolVar(&dry, "dry-run", false, "Preview installation without writing")
			c.Flags().BoolVar(&overwrite, "overwrite", false, "Replace differing installed content after reviewing it")
		}
		g.AddCommand(c)
	}
	return g
}

func skillPaths(agent, dir string, project bool) ([]string, error) {
	if dir != "" {
		if agent != "" || project {
			return nil, e.New(e.CategoryUsage, "SKILL_FLAGS", "--dir cannot be combined with --agent or --project.")
		}
		p, err := filepath.Abs(dir)
		if err != nil {
			return nil, e.New(e.CategoryConfig, "SKILL_PATH", "Cannot resolve Skill directory.")
		}
		return []string{filepath.Join(p, "wecom-channel")}, nil
	}
	base, err := os.UserHomeDir()
	if project {
		base, err = os.Getwd()
	}
	if err != nil {
		return nil, e.New(e.CategoryConfig, "SKILL_PATH", "Cannot locate agent directories.")
	}
	agents := kit.Detect(os.DirFS(base), project)
	if agent != "" {
		v, ok := kit.AgentByID(agent)
		if !ok {
			return nil, e.New(e.CategoryUsage, "SKILL_AGENT", "Unknown agent ID.")
		}
		agents = []kit.Agent{v}
	}
	out := []string{}
	for _, a := range agents {
		p := a.Destination(base, "wecom-channel", false)
		if project {
			p = a.Destination("", "wecom-channel", true)
			p = filepath.Join(base, p)
		}
		out = append(out, p)
	}
	return out, nil
}

func installSkill(path string, data []byte, dry, overwrite bool) error {
	for p := path; p != "/"; p = filepath.Dir(p) {
		if i, err := os.Lstat(p); err == nil {
			if i.Mode()&os.ModeSymlink != 0 || !i.IsDir() {
				return e.New(e.CategoryConfig, "SKILL_PATH", "Skill installation refuses symlink or non-directory parents.")
			}
		} else if !os.IsNotExist(err) {
			return e.New(e.CategoryConfig, "SKILL_PATH", "Cannot inspect Skill destination.")
		}
	}
	target := filepath.Join(path, "SKILL.md")
	if i, err := os.Lstat(target); err == nil {
		if !i.Mode().IsRegular() {
			return e.New(e.CategoryConflict, "SKILL_EXISTS", "Skill destination is not a regular file.")
		}
		old, err := os.ReadFile(target)
		if err != nil {
			return e.New(e.CategoryConfig, "SKILL_READ", "Cannot inspect installed Skill.")
		}
		if bytes.Equal(old, data) {
			return nil
		}
		if !overwrite {
			return e.New(e.CategoryConflict, "SKILL_EXISTS", "Installed Skill content differs; inspect it before using --overwrite.")
		}
	} else if !os.IsNotExist(err) {
		return e.New(e.CategoryConfig, "SKILL_READ", "Cannot inspect Skill destination.")
	}
	if dry {
		return nil
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return e.New(e.CategoryConfig, "SKILL_WRITE", "Cannot create Skill directory.")
	}
	if err := state.CheckDir(path, false); err != nil {
		return err
	}
	f, err := os.CreateTemp(path, ".skill-*")
	if err != nil {
		return e.New(e.CategoryConfig, "SKILL_WRITE", "Cannot write Skill.")
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(fs.FileMode(0644)); err == nil {
		_, err = f.Write(data)
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil || os.Rename(name, target) != nil {
		return e.New(e.CategoryConfig, "SKILL_WRITE", "Cannot save Skill.")
	}
	return nil
}
