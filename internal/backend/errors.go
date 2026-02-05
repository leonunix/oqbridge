package backend

import "fmt"

// HTTPStatusError represents a non-2xx response from a backend HTTP call.
// It preserves the status code for callers that need to make security decisions
// (e.g. differentiate 401/403 from transient backend failures).
type HTTPStatusError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.URL == "" {
		return fmt.Sprintf("http status %d", e.StatusCode)
	}
	if e.Body == "" {
		return fmt.Sprintf("http %s returned status %d", e.URL, e.StatusCode)
	}
	return fmt.Sprintf("http %s returned status %d: %s", e.URL, e.StatusCode, e.Body)
}
