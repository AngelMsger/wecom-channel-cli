// Package errors re-exports the family error contract with channel guidance.
package errors

import kit "github.com/angelmsger/agent-cli-kit/errors"

type Category = kit.Category
type CLIError = kit.CLIError
type Payload = kit.Payload

const (
	CategoryUsage      = kit.CategoryUsage
	CategoryConfig     = kit.CategoryConfig
	CategoryAuth       = kit.CategoryAuth
	CategoryPermission = kit.CategoryPermission
	CategoryNotFound   = kit.CategoryNotFound
	CategoryConflict   = kit.CategoryConflict
	CategoryRateLimit  = kit.CategoryRateLimit
	CategoryNetwork    = kit.CategoryNetwork
	CategoryServer     = kit.CategoryServer
	CategoryParse      = kit.CategoryParse
	CategoryInternal   = kit.CategoryInternal
)

var New = kit.New
var Newf = kit.Newf
var AsCLIError = kit.AsCLIError
var ExitCode = kit.ExitCode

func init() {
	kit.SetGuidance(func(c kit.Category) (string, []string) {
		if c == kit.CategoryAuth {
			return "Ask the broker operator to verify API-mode credentials and the host clock.", []string{"wecom-channel-cli auth guide"}
		}
		return "Inspect the command and broker policy; do not retry an uncertain write.", []string{"wecom-channel-cli --help"}
	})
}

// Safe prevents arbitrary network, filesystem or parser errors leaking secrets.
func Safe(err error) *CLIError {
	if e, ok := err.(*CLIError); ok {
		return e
	}
	return New(CategoryInternal, "INTERNAL", "The operation failed locally; no raw diagnostics are exposed.")
}
