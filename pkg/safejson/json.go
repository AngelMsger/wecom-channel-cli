// Package safejson rejects ambiguous JSON at security boundaries.
package safejson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// Decode rejects duplicate keys, excessive nesting and trailing values.
func Decode(data []byte, out any, strict bool) error {
	if !utf8.Valid(data) {
		return errors.New("invalid UTF-8 JSON")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 32 {
			return errors.New("JSON is too deeply nested")
		}
		t, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					t, err := d.Token()
					if err != nil {
						return err
					}
					key, ok := t.(string)
					if !ok || seen[key] {
						return errors.New("ambiguous JSON key")
					}
					seen[key] = true
					if err := value(depth + 1); err != nil {
						return err
					}
				}
			case '[':
				for d.More() {
					if err := value(depth + 1); err != nil {
						return err
					}
				}
			default:
				return errors.New("invalid JSON delimiter")
			}
			_, err = d.Token()
			return err
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if strict {
		d.DisallowUnknownFields()
	}
	return d.Decode(out)
}
