package queuefeed

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed/queuebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type readerState int

const (
	readerStateBatching readerState = iota
	readerStateHasUncommittedBatch
	readerStateCheckingForReassignment
	readerStateDead
)

// - [x] setup rangefeed on data
// - [X] handle only watching my partitions
// - [X] after each batch, ask mgr if i need to  assignments
// - [X] buffer rows in the background before being asked for them
// - [ ] checkpoint frontier if our frontier has advanced and we confirmed receipt
// - [X] gonna need some way to clean stuff up on conn_executor.close()
//
// has rangefeed on data. reads from it. handles handoff
// state machine around handing out batches and handing stuff off
type Reader struct {
	executor isql.DB
	rff      *rangefeed.Factory
	mgr      *Manager
	name     string
	assigner *PartitionAssignments

	// stuff for decoding data. this is ripped from rowfetcher_cache.go in changefeeds
	codec    keys.SQLCodec
	leaseMgr *lease.Manager

	mu struct {
		syncutil.Mutex
		inflightBuffer []bufferedEvent
	}

	assignment *Assignment
	tablefeed  *tableFeed
}

func NewReader(
	ctx context.Context,
	executor isql.DB,
	mgr *Manager,
	rff *rangefeed.Factory,
	codec keys.SQLCodec,
	table descpb.ID,
	leaseMgr *lease.Manager,
	session Session,
	assigner *PartitionAssignments,
	name string,
) (*Reader, error) {
	r := &Reader{
		executor: executor,
		mgr:      mgr,
		codec:    codec,
		leaseMgr: leaseMgr,
		name:     name,
		rff:      rff,
		// stored so we can use it in methods using a different context than the main goro ie GetRows and ConfirmReceipt
		goroCtx:  ctx,
		assigner: assigner,
	}

	assignment, err := r.waitForAssignment(ctx, session)
	if err != nil {
		return nil, errors.Wrap(err, "waiting for assignment")
	}
	if err := r.setupTablefeed(ctx, assignment); err != nil {
		return nil, errors.Wrap(err, "setting up rangefeed")
	}

	return r, nil
}

var ErrNoPartitionsAssigned = errors.New("no partitions assigned to reader: todo support this case by polling for assignment")

func (r *Reader) waitForAssignment(ctx context.Context, session Session) (*Assignment, error) {
	// We can rapidly poll this because the assigner has an in-memory cache of
	// assignments.
	//
	// TODO: should this retry loop be in RegisterSession instead?
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		assignment, err := r.assigner.RegisterSession(ctx, session)
		if err != nil {
			return nil, errors.Wrap(err, "registering session for reader")
		}
		if len(assignment.Partitions) != 0 {
			return assignment, nil
		}

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "waiting for assignment")
		case <-timer.C:
			// continue
		}
	}
}

func (r *Reader) setupTablefeed(ctx context.Context, assignment *Assignment) error {
	// TODO: handle the case where there are no partitions in the assignment. In
	// that case we should poll `RefreshAssignment` until we get one. This would
	// only occur if every assignment was handed out already.
	//
	// This case is pretty uncommon since we wait for an assignment with at least
	// one on construction, but it could happen if a race occurs and all of a
	// reader's partitions are stolen.
	if len(assignment.Partitions) == 0 {
		return errors.Wrap(ErrNoPartitionsAssigned, "setting up rangefeed")
	}

	frontier, err := span.MakeFrontier(assignment.Spans()...)
	if err != nil {
		return errors.Wrap(err, "creating frontier")
	}

	for _, partition := range assignment.Partitions {
		checkpointTS, err := r.mgr.ReadCheckpoint(ctx, r.name, partition.ID)
		if err != nil {
			return errors.Wrapf(err, "reading checkpoint for partition %d", partition.ID)
		}
		if !checkpointTS.IsEmpty() {
			_, err := frontier.Forward(partition.Span, checkpointTS)
			if err != nil {
				return errors.Wrapf(err, "advancing frontier for partition %d to checkpoint %s", partition.ID, checkpointTS)
			}
		} else {
			return errors.Errorf("checkpoint is empty for partition %d", partition.ID)
		}
	}

	if frontier.Frontier().IsEmpty() {
		return errors.New("frontier is empty")
	}

	rf, err := newTableFeed(
		ctx,
		fmt.Sprintf("queue=%s", r.name),
		r.tablefeed.tableID,
		frontier,
		r.rff,
		r.leaseMgr,
		&r.codec,
	)
	if err != nil {
		return errors.Wrap(err, "creating tablefeed")
	}

	r.tablefeed = rf
	r.assignment = assignment

	return nil
}

func (r *Reader) GetRows(ctx context.Context, limit int) ([]tree.Datums, error) {
	fmt.Printf("GetRows start\n")
	for {
		datums, err := func() ([]tree.Datums, error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			if len(r.mu.inflightBuffer) == 0 {
				var err error
				r.mu.inflightBuffer, err = r.mu.ReadBufferedEvents(r.mu.inflightBuffer)
				if err != nil {
					return nil, errors.Wrap(err, "queuefeed reading from tablefeed")
				}
				// If we have an in-flight batch, hand these rows out first
			}

			if len(r.mu.inflightBuffer) != 0 {
				// TODO convert to datums and return these to the user
			}
		}()
	}

	if r.isShutdown.Load() {
		return nil, errors.New("reader is shutting down")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateBatching {
		return nil, errors.New("reader not idle")
	}
	if len(r.mu.inflightBuffer) > 0 {
		return nil, errors.AssertionFailedf("getrows called with nonempty inflight buffer")
	}

	// Helper to count data events (not checkpoints) in buffer
	hasDataEvents := func() bool {
		for _, event := range r.mu.buf {
			if event.resolved.IsEmpty() {
				return true
			}
		}
		return false
	}

	// Wait until we have at least one data event (not just checkpoints)
	if !hasDataEvents() {
		// shut down the reader if this ctx (which is distinct from the goro ctx) is canceled
		defer context.AfterFunc(ctx, func() {
			r.cancel(errors.Wrapf(ctx.Err(), "GetRows canceled"))
		})()
		for ctx.Err() == nil && r.goroCtx.Err() == nil && !hasDataEvents() {
			r.mu.pushedWakeup.Wait()
		}
		if ctx.Err() != nil {
			return nil, errors.Wrapf(ctx.Err(), "GetRows canceled")
		}
	}

	// Find the position of the (limit+1)th data event (not checkpoint)
	// We'll take everything up to that point, which gives us up to `limit` data rows
	// plus any checkpoints that came before/between them.
	bufferEndIdx := len(r.mu.buf)

	// Optimization: if the entire buffer is smaller than limit, take it all
	if len(r.mu.buf) > limit {
		dataCount := 0
		for i, event := range r.mu.buf {
			if event.resolved.IsEmpty() {
				dataCount++
				if dataCount > limit {
					bufferEndIdx = i
					break
				}
			}
		}
	}

	r.mu.inflightBuffer = append(r.mu.inflightBuffer, r.mu.buf[0:bufferEndIdx]...)
	r.mu.buf = r.mu.buf[bufferEndIdx:]

	r.mu.state = readerStateHasUncommittedBatch
	r.mu.poppedWakeup.Broadcast()

	// Here we filter to return only data events to the user.
	result := make([]tree.Datums, 0, limit)
	for _, event := range r.mu.inflightBuffer {
		if event.resolved.IsEmpty() {
			result = append(result, event.row)
		}
	}

	return result, nil
}

// ConfirmReceipt is called when we commit a transaction that reads from the queue.
// We will checkpoint if we have checkpoint events in our inflightBuffer.
func (r *Reader) ConfirmReceipt(ctx context.Context) {
	if r.isShutdown.Load() {
		return
	}

	var checkpointToWrite hlc.Timestamp
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		// Find the last checkpoint in inflightBuffer
		for _, event := range r.mu.inflightBuffer {
			if !event.resolved.IsEmpty() {
				checkpointToWrite = event.resolved
			}
		}

		r.mu.inflightBuffer = r.mu.inflightBuffer[:0]
		r.mu.state = readerStateCheckingForReassignment
	}()

	// Persist the checkpoint if we have one.
	if !checkpointToWrite.IsEmpty() {
		for _, partition := range r.assignment.Partitions {
			if err := r.mgr.WriteCheckpoint(ctx, r.name, partition.ID, checkpointToWrite); err != nil {
				fmt.Printf("error writing checkpoint for partition %d: %s\n", partition.ID, err)
				// TODO: decide how to handle checkpoint write errors. Since the txn
				// has already committed, I don't think we can really fail at this point.
			}
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-r.goroCtx.Done():
		return
	default:
		// TODO only set caughtUp to true if our frontier is near the current time.
		newAssignment, err := r.assigner.RefreshAssignment(ctx, r.assignment, true /*=caughtUp*/)
		if err != nil {
			r.cancel(errors.Wrap(err, "refreshing assignment"))
			return
		}
		if newAssignment != nil {
			if err := r.updateAssignment(newAssignment); err != nil {
				r.cancel(errors.Wrap(err, "updating assignment"))
				return
			}
		}
	}
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.state = readerStateBatching
	}()
}

func (r *Reader) RollbackBatch(ctx context.Context) {
	if r.isShutdown.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	newBuf := make([]bufferedEvent, 0, len(r.mu.inflightBuffer)+len(r.mu.buf))
	newBuf = append(newBuf, r.mu.inflightBuffer...)
	newBuf = append(newBuf, r.mu.buf...)
	r.mu.buf = newBuf
	r.mu.inflightBuffer = r.mu.inflightBuffer[:0]

	r.mu.state = readerStateBatching
}

func (r *Reader) IsAlive() bool {
	return !r.isShutdown.Load()
}

func (r *Reader) Close() error {
	err := r.assigner.UnregisterSession(r.goroCtx, r.session)
	r.cancel(errors.New("reader closing"))
	r.rangefeed.Close()
	return err
}

func (r *Reader) updateAssignment(assignment *Assignment) error {
	defer func() {
		fmt.Printf("updateAssignment done with assignment: %+v\n", assignment)
	}()

	r.rangefeed.Close()
	r.assignment = assignment

	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.buf = r.mu.buf[:0]
	}()

	if err := r.setupRangefeed(r.goroCtx, assignment); err != nil {
		return errors.Wrapf(err, "setting up rangefeed for new assignment: %+v", assignment)
	}
	return nil
}

func (r *Reader) checkForReassignment(ctx context.Context) error {
	defer func() {
		fmt.Println("checkForReassignment done")
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateCheckingForReassignment {
		return errors.AssertionFailedf("reader not in checking for reassignment state")
	}

	change, err := r.mgr.reassessAssignments(ctx, r.name)
	if err != nil {
		return errors.Wrap(err, "reassessing assignments")
	}
	if change {
		fmt.Println("TODO: reassignment detected. lets do something about it")
	}
	r.mu.state = readerStateBatching
	return nil
}

var _ queuebase.Reader = &Reader{}
