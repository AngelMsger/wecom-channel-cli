package apiclient

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"

	e "github.com/angelmsger/wecom-channel-cli/pkg/errors"
)

// assertCoversMethods fails unless names is exactly the method set of iface,
// so a table test cannot silently skip a method added to the interface.
func assertCoversMethods(t *testing.T, iface reflect.Type, names []string) {
	t.Helper()
	var want []string
	for i := range iface.NumMethod() {
		want = append(want, iface.Method(i).Name)
	}
	got := slices.Sorted(slices.Values(names))
	if !slices.Equal(got, want) {
		t.Fatalf("table covers %v, %s declares %v", got, iface.Name(), want)
	}
}

// errCannedRead is returned alongside every canned read result, so delegation
// tests also see errors pass through unchanged.
var errCannedRead = errors.New("canned read error")

// recordingClient answers every method with a fixed result and records the
// call, so tests can see exactly what reached the inner client.
type recordingClient struct {
	reads  []Query
	writes []WriteRequest
}

func (r *recordingClient) Read(_ context.Context, q Query) (Page, error) {
	r.reads = append(r.reads, q)
	return Page{Next: "canned", HasMore: true}, errCannedRead
}

func (r *recordingClient) Send(_ context.Context, w WriteRequest) (SendResult, error) {
	r.writes = append(r.writes, w)
	return SendResult{ID: "sent", Kind: w.Kind}, nil
}

// TestReadOnlyBlocksEveryWrite drives every Writer method on the wrapper and
// asserts each one returns a READONLY_BLOCKED permission error without
// reaching the inner client.
func TestReadOnlyBlocksEveryWrite(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(Client) error{
		"Send": func(c Client) error {
			_, err := c.Send(ctx, WriteRequest{Kind: "post", Channel: "c", Content: "text", Key: "k"})
			return err
		},
	}
	assertCoversMethods(t, reflect.TypeFor[Writer](), slices.Collect(maps.Keys(cases)))

	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			inner := &recordingClient{}
			ce := e.AsCLIError(write(NewReadOnly(inner)))
			if ce == nil || ce.Category != e.CategoryPermission || ce.Code != "READONLY_BLOCKED" {
				t.Fatalf("got %v, want permission/READONLY_BLOCKED", ce)
			}
			if len(inner.writes) != 0 || len(inner.reads) != 0 {
				t.Fatalf("inner client received reads %v, writes %v", inner.reads, inner.writes)
			}
		})
	}
}

// TestReadOnlyDelegatesEveryRead calls every Reader method through the wrapper
// and asserts the inner client received exactly that one call with the same
// arguments, and that its results and error came back unchanged.
func TestReadOnlyDelegatesEveryRead(t *testing.T) {
	ctx := context.Background()
	query := func() Query {
		return Query{Operation: "post.list", Channel: "c", Cursor: "next", Limit: 7, ChannelIDs: []string{"c"}}
	}
	cases := map[string]func(Client) []any{
		"Read": func(c Client) []any {
			page, err := c.Read(ctx, query())
			return []any{page, err}
		},
	}
	assertCoversMethods(t, reflect.TypeFor[Reader](), slices.Collect(maps.Keys(cases)))

	for name, read := range cases {
		t.Run(name, func(t *testing.T) {
			inner := &recordingClient{}
			got := read(NewReadOnly(inner))
			if want := read(&recordingClient{}); !reflect.DeepEqual(got, want) {
				t.Errorf("results = %#v, want the inner client's %#v", got, want)
			}
			if want := []Query{query()}; !reflect.DeepEqual(inner.reads, want) || len(inner.writes) != 0 {
				t.Errorf("inner client received reads %#v, writes %#v; want reads %#v", inner.reads, inner.writes, want)
			}
		})
	}
}
