package postings

import (
	"sync"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/xcap"
)

// LocalityAccumulator sums column_name page-locality across scan passes. The
// stream selector walks a section's column_name column once per pass
// (match/filter/admit), so observations are deduped per section — the first
// observation for a section wins. Because every pass queries the same set of
// label names, relevant/runs are stable across passes, so first-wins yields the
// section's true page count and a representative relevance/fragmentation.
//
// A LocalityAccumulator is safe for concurrent use; admission scans run
// per-section concurrently.
type LocalityAccumulator struct {
	mu            sync.Mutex
	seen          map[*Section]struct{}
	pagesTotal    int64
	pagesRelevant int64
	pageRuns      int64
}

// NewLocalityAccumulator returns an empty accumulator.
func NewLocalityAccumulator() *LocalityAccumulator {
	return &LocalityAccumulator{seen: make(map[*Section]struct{})}
}

// observe folds one column_name page-prune observation for sec into the totals,
// ignoring repeat observations for a section already seen.
func (a *LocalityAccumulator) observe(sec *Section, s dataset.PagePruneStats) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[sec]; ok {
		return
	}
	a.seen[sec] = struct{}{}
	a.pagesTotal += int64(s.TotalPages)
	a.pagesRelevant += int64(s.RelevantPages)
	a.pageRuns += int64(s.PageRuns)
}

// Record observes the accumulated column_name page-locality onto span as the
// postings.column_name.pages.* statistics.
func (a *LocalityAccumulator) Record(span *xcap.Span) {
	if a == nil || span == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	span.Record(StatColumnNamePagesTotal.Observe(a.pagesTotal))
	span.Record(StatColumnNameRelevantPages.Observe(a.pagesRelevant))
	span.Record(StatColumnNamePageRuns.Observe(a.pageRuns))
}
