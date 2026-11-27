package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/angelmsger/agent-cli-kit/terminal"
	"github.com/angelmsger/wecom-channel-cli/internal/securefs"
	"github.com/angelmsger/wecom-channel-cli/pkg/apiclient"
	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
	"github.com/angelmsger/wecom-channel-cli/pkg/safejson"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

const maxProfileBytes = 4 << 20

type personalChannel struct {
	ID       string `json:"topic_id"`
	Name     string `json:"topic_name"`
	Created  int64  `json:"create_time,omitempty"`
	Selected bool   `json:"selected"`
}

type personalProfile struct {
	Version  int               `json:"version"`
	Bot      string            `json:"bot_id"`
	Channels []personalChannel `json:"channels"`
}

func personalPath(dir string) string { return filepath.Join(dir, "channels.json") }

func readPersonal(dir string) (personalProfile, error) {
	var p personalProfile
	data, err := securefs.ReadPrivate(personalPath(dir), maxProfileBytes, false)
	if os.IsNotExist(err) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	if safejson.Decode(data, &p, true) != nil || p.Version != 1 || !apiclient.Identifier(p.Bot, 256) {
		return p, e.New(e.CategoryConfig, "PERSONAL_PROFILE", "The saved personal connection is invalid.")
	}
	seen := make(map[string]bool)
	for _, ch := range p.Channels {
		if !apiclient.Identifier(ch.ID, 128) || seen[ch.ID] {
			return p, e.New(e.CategoryConfig, "PERSONAL_PROFILE", "The saved channel catalog is invalid.")
		}
		seen[ch.ID] = true
	}
	return p, nil
}

func (p personalProfile) encode() ([]byte, error) {
	data, err := json.Marshal(p)
	if err != nil || len(data) > maxProfileBytes {
		return nil, e.New(e.CategoryConfig, "CHANNEL_CATALOG_LIMIT", "The channel catalog exceeds the personal profile size limit.")
	}
	return data, nil
}

// Keep the old catalog until the entire new one is available. Names are labels;
// stable IDs preserve selections and determine private state paths.
func discoverPersonal(ctx context.Context, api apiclient.Reader, old personalProfile, bot string) (personalProfile, error) {
	p := personalProfile{Version: 1, Bot: bot, Channels: []personalChannel{}}
	selected := make(map[string]bool)
	for _, ch := range old.Channels {
		selected[ch.ID] = ch.Selected
	}
	seenIDs, cursors := make(map[string]bool), make(map[string]bool)
	q := apiclient.Query{Operation: "channel.list", Limit: 50}
	var labelBytes int
	for {
		page, err := api.Read(ctx, q)
		if err != nil {
			return p, err
		}
		for _, record := range page.Items {
			ch, ok := record.Data.(apiclient.Topic)
			if !ok || !apiclient.Identifier(ch.ID, 128) || seenIDs[ch.ID] {
				return p, e.New(e.CategoryParse, "CHANNEL_CATALOG_CHANGED", "Channel discovery returned inconsistent entries. Run connect again to refresh.")
			}
			labelBytes += len(ch.ID) + len(ch.Name)
			if labelBytes > maxProfileBytes {
				return p, e.New(e.CategoryConfig, "CHANNEL_CATALOG_LIMIT", "The channel catalog exceeds the personal profile size limit.")
			}
			seenIDs[ch.ID] = true
			p.Channels = append(p.Channels, personalChannel{ID: ch.ID, Name: ch.Name, Created: ch.Created, Selected: selected[ch.ID]})
		}
		if !page.HasMore {
			break
		}
		if !apiclient.Identifier(page.Next, 256) || cursors[page.Next] || len(cursors) >= 200 {
			return p, e.New(e.CategoryParse, "CHANNEL_DISCOVERY_INCOMPLETE", "Discovery did not complete within the page bound. The saved channel list is unchanged.")
		}
		cursors[page.Next] = true
		q.Cursor = page.Next
	}
	sort.Slice(p.Channels, func(i, j int) bool {
		a, b := p.Channels[i], p.Channels[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ID < b.ID
	})
	return p, nil
}

func (p personalProfile) choices() []apiclient.Record {
	rows := make([]apiclient.Record, 0, len(p.Channels))
	for i, ch := range p.Channels {
		rows = append(rows, apiclient.Untrusted(struct {
			personalChannel
			Choice int `json:"choice"`
		}{ch, i + 1}, ch.ID, "", ""))
	}
	return rows
}

type personalSelection struct {
	names     []string
	picks     []int
	all, none bool
}

func (s personalSelection) explicit() bool { return s.all || s.none || len(s.names)+len(s.picks) > 0 }
func (s *personalSelection) flags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringArrayVar(&s.names, "channel", nil, "Select an exact discovered channel name; repeat to select several")
	f.IntSliceVar(&s.picks, "pick", nil, "Select numbered entries from the last saved channel list (for duplicate names)")
	f.BoolVar(&s.all, "all", false, "Select all currently discovered channels")
	f.BoolVar(&s.none, "none", false, "Clear the saved selection")
}
func (s personalSelection) apply(p *personalProfile) error {
	if (s.all && s.none) || ((s.all || s.none) && len(s.names)+len(s.picks) > 0) {
		return e.New(e.CategoryUsage, "CHANNEL_SELECTION", "Choose names/numbers, --all, or --none.")
	}
	chosen := make(map[int]bool)
	for _, pick := range s.picks {
		if pick < 1 || pick > len(p.Channels) {
			return e.New(e.CategoryUsage, "CHANNEL_CHOICE", "Choose a number from the channel list returned by connect.")
		}
		chosen[pick-1] = true
	}
	for _, name := range s.names {
		var matches []int
		for i, ch := range p.Channels {
			if ch.Name == name {
				matches = append(matches, i+1)
			}
		}
		switch len(matches) {
		case 0:
			return e.New(e.CategoryNotFound, "CHANNEL_NAME_UNKNOWN", "The channel name was not found. Refresh with connect and select a listed name.")
		case 1:
			chosen[matches[0]-1] = true
		default:
			return e.New(e.CategoryUsage, "CHANNEL_NAME_AMBIGUOUS", "Several channels have this name. Select a matching --pick number.").WithDetails(map[string]any{"choices": matches})
		}
	}
	for i := range p.Channels {
		p.Channels[i].Selected = s.all || chosen[i]
	}
	return nil
}
func (p personalProfile) selected() []personalChannel {
	return slices.DeleteFunc(slices.Clone(p.Channels), func(ch personalChannel) bool { return !ch.Selected })
}
func promptSelection(cmd *cobra.Command, p personalProfile) (personalSelection, error) {
	var s personalSelection
	for i, ch := range p.Channels {
		fmt.Fprintf(cmd.ErrOrStderr(), "%d  %q  (created %d)\n", i+1, ch.Name, ch.Created)
	}
	answer, err := (terminal.Prompter{In: cmd.InOrStdin(), Out: cmd.ErrOrStderr()}).Line("Choose channel numbers separated by commas, all, or none", "")
	if err != nil {
		return s, err
	}
	switch answer {
	case "all":
		s.all = true
	case "none":
		s.none = true
	default:
		for _, part := range strings.Split(answer, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return s, e.New(e.CategoryUsage, "CHANNEL_CHOICE", "Enter channel numbers, all, or none.")
			}
			s.picks = append(s.picks, n)
		}
	}
	return s, nil
}

func lockPersonalProfile(dir string) (*os.File, error) {
	f, err := securefs.LockPrivate(filepath.Join(dir, "connection.lock"))
	if err == unix.EWOULDBLOCK {
		return nil, e.New(e.CategoryConflict, "PERSONAL_IN_USE", "Another connect or foreground listener is active. Stop it before changing this connection.")
	}
	return f, err
}
