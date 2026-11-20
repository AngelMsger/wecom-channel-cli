package apiclient

import e "github.com/angelmsger/wecom-channel-cli/pkg/errors"

// RequestPhase identifies the step at which a WeCom operation stopped.
type RequestPhase string

const (
	PhasePrepare         RequestPhase = "prepare"
	PhaseTokenBootstrap  RequestPhase = "token_bootstrap"
	PhaseBusinessRequest RequestPhase = "business_request"
)

// RequestFailure records how far a WeCom operation progressed. PhaseBusinessRequest starts
// when the client hands the request to the transport, before its outcome is known.
type RequestFailure struct {
	Phase           RequestPhase `json:"phase"`
	Code            string       `json:"code"`
	UpstreamErrcode *int         `json:"upstream_errcode,omitempty"`
}

func (f *RequestFailure) Error() string { return string(f.Phase) + ": " + f.Code }

// Unsubmitted reports whether the business endpoint could not have been called.
func (f *RequestFailure) Unsubmitted() bool {
	return f.Phase == PhasePrepare || f.Phase == PhaseTokenBootstrap
}

// Keep the CLI error and its transport details available to existing readers.
func (phase RequestPhase) failed(err error, upstreamCode *int) *e.CLIError {
	cause := e.Safe(err)
	return cause.WithCause(&RequestFailure{Phase: phase, Code: cause.Code, UpstreamErrcode: upstreamCode})
}
