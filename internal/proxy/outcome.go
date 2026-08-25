package proxy

type Outcome string

const (
	OutcomeSuccess            Outcome = "success"
	OutcomeUpstreamError      Outcome = "upstream_error"
	OutcomeIncomplete         Outcome = "incomplete"
	OutcomeClientCancel       Outcome = "client_cancel"
	OutcomeShutdown           Outcome = "shutdown"
	OutcomeObservationLimited Outcome = "observation_limited"
	OutcomeInternalError      Outcome = "internal_error"
)

func (o Outcome) failed() bool {
	return o == OutcomeUpstreamError || o == OutcomeIncomplete || o == OutcomeInternalError
}
