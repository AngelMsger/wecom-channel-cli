package safejson

import "testing"

func TestRejectAmbiguousJSON(t *testing.T) {
	for _, s := range []string{`{"admin":false,"admin":true}`, `{"from":{"userid":"a","userid":"b"}}`, `{} {}`, `{"x":[{"x":1,"x":2}]}`, `{"x":`} {
		var v any
		if Decode([]byte(s), &v, false) == nil {
			t.Errorf("accepted %s", s)
		}
	}
	var v map[string]any
	if err := Decode([]byte(`{"x":[1,{"safe":"data"}]}`), &v, true); err != nil {
		t.Fatal(err)
	}
}
