package proxy

import (
	"time"

	"github.com/leonunix/oqbridge/internal/util"
)

// RouteTarget indicates where a query should be routed.
type RouteTarget int

const (
	RouteHotOnly  RouteTarget = iota // Query only OpenSearch (hot data).
	RouteColdOnly                    // Query only Quickwit (cold data).
	RouteBoth                        // Query both backends and merge results.
)

func (r RouteTarget) String() string {
	switch r {
	case RouteHotOnly:
		return "hot_only"
	case RouteColdOnly:
		return "cold_only"
	case RouteBoth:
		return "both"
	default:
		return "unknown"
	}
}

// Router determines the query routing target based on time range analysis.
type Router struct {
	retentionDays int
}

// NewRouter creates a new Router with the given retention threshold.
func NewRouter(retentionDays int) *Router {
	return &Router{retentionDays: retentionDays}
}

// Route analyzes the query body and decides where to send it.
func (r *Router) Route(body []byte, timestampField string) RouteTarget {
	tr := util.ExtractTimeRange(body, timestampField)
	if tr == nil {
		// Cannot determine time range — query both backends to be safe.
		return RouteBoth
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -r.retentionDays)

	hasHot := false
	hasCold := false

	// Determine if the range overlaps with hot or cold tiers.
	if tr.From != nil && tr.To != nil {
		if tr.To.Before(cutoff) {
			hasCold = true
		} else if tr.From.After(cutoff) || tr.From.Equal(cutoff) {
			hasHot = true
		} else {
			hasHot = true
			hasCold = true
		}
	} else if tr.From != nil {
		// Open-ended to the future.
		if tr.From.Before(cutoff) {
			hasHot = true
			hasCold = true
		} else {
			hasHot = true
		}
	} else if tr.To != nil {
		// Open-ended to the past.
		if tr.To.Before(cutoff) {
			hasCold = true
		} else {
			hasHot = true
			hasCold = true
		}
	}

	if hasHot && hasCold {
		return RouteBoth
	}
	if hasCold {
		return RouteColdOnly
	}
	return RouteHotOnly
}
