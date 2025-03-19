package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

type cruddyBatchHandler struct {
}

// Close implements BatchHandler.
func (c *cruddyBatchHandler) Close(context.Context) {
	panic("unimplemented")
}

// GetLastRow implements BatchHandler.
func (c *cruddyBatchHandler) GetLastRow() cdcevent.Row {
	panic("unimplemented")
}

// HandleBatch implements BatchHandler.
func (c *cruddyBatchHandler) HandleBatch(
	context.Context, []streampb.StreamEvent_KV,
) (batchStats, error) {
	panic("unimplemented")
}

// ReleaseLeases implements BatchHandler.
func (c *cruddyBatchHandler) ReleaseLeases(context.Context) {
	panic("unimplemented")
}

// ReportMutations implements BatchHandler.
func (c *cruddyBatchHandler) ReportMutations(*stats.Refresher) {
	panic("unimplemented")
}

// SetSyntheticFailurePercent implements BatchHandler.
func (c *cruddyBatchHandler) SetSyntheticFailurePercent(uint32) {
	panic("unimplemented")
}

var _ BatchHandler = &cruddyBatchHandler{}
