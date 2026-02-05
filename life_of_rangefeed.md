# Life of a Rangefeed

Generated: 2026-02-04
Repository: https://github.com/cockroachdb/cockroach
Commit: 8385dd71290393b5bc5ec4f3f9d2dab183743c26
Branch: master

## Introduction

This document traces the execution of a rangefeed through the CockroachDB codebase, following the complete lifecycle from client initialization through event delivery. A rangefeed is CockroachDB's mechanism for streaming real-time change notifications from the database to clients, providing eventual consistency guarantees through resolved timestamps.

Rangefeeds power critical features including:
- Changefeeds (CDC - Change Data Capture)
- Schema change notifications
- Distributed cache invalidation
- Real-time materialized view updates

This trace follows a rangefeed from initial client request through:
1. **Client Setup** - Application creates rangefeed via Factory
2. **DistSender Routing** - Request divided across range boundaries
3. **Mux RPC Multiplexing** - Efficient connection management per node
4. **Server-Side Processing** - Replica rangefeed registration and event generation
5. **Event Delivery** - Streaming events back to client

## Important Constraints

Rangefeeds have specific limitations documented in the code:

- Do NOT support inline (unversioned) values
- Will ERROR if MVCC history is mutated via operations like ClearRange
- Initial timestamp is EXCLUSIVE (first event at `initialTimestamp.Next()`)
- GC threshold must be respected for catch-up scans

---

## Phase 1: Client Initialization

### Entry Point: RangeFeed Factory

The rangefeed journey begins when a client creates a [`Factory`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L80-L85) and calls [`RangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L138-L151):

```go
// Factory is used to construct RangeFeeds.
type Factory struct {
	stopper *stop.Stopper
	client  DB
	knobs   *TestingKnobs
}
```

The factory is created via [`NewFactory()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L105-L114), which wraps a `kv.DB` instance with rangefeed-specific functionality.

### RangeFeed Structure

A [`RangeFeed`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L176-L193) represents an active rangefeed session:

```go
type RangeFeed struct {
	config
	name    string
	client  DB
	stopper *stop.Stopper
	knobs   *TestingKnobs

	initialTimestamp hlc.Timestamp
	spans            []roachpb.Span
	spansDebugStr    string

	onValue OnValue

	cancel  context.CancelFunc
	running sync.WaitGroup
	started int32 // accessed atomically
}
```

### Startup Sequence

When [`Start()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L198-L211) is called, it:

1. Creates a span frontier to track progress across all spans
2. Launches an async task running [`run()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L307-L430)
3. Optionally performs an initial scan via [`runInitialScan()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L312-L315)
4. Establishes the actual rangefeed stream

The run loop implements automatic retry with exponential backoff:
- Successful runs > 30 seconds reset backoff state
- Transient errors (network, lease changes) trigger retry
- Unrecoverable errors (GC threshold, MVCC mutations) propagate to `onUnrecoverableError`

---

## Phase 2: DistSender Routing

### Dividing Spans on Range Boundaries

The client's target spans rarely align with CockroachDB's range boundaries. The DistSender must divide these spans appropriately.

At [`dist_sender_rangefeed.go:181`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L181-L208), the [`RangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L181) method:

```go
func (ds *DistSender) RangeFeed(
	ctx context.Context,
	spans []SpanTimePair,
	eventCh chan<- RangeFeedMessage,
	opts ...RangeFeedOption,
) error
```

This creates a [`rangeFeedRegistry`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L376-L393) to track all partial rangefeeds, then delegates to [`muxRangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L60-L95).

### Range Boundary Division

The [`divideAllSpansOnRangeBoundaries()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L210-L239) function:

1. **Sorts spans by start timestamp** (oldest first) at [line 225](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L225-L227)
   - This prevents starvation during throttled catch-up scans
   - Ensures oldest spans get priority to complete catch-up

2. **Iterates ranges** using [`divideSpanOnRangeBoundaries()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L432-L462) at line 234
   - Uses [`RangeIterator`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L446) to walk range descriptors
   - Computes intersection of request span with each range
   - Invokes callback for each partial span

3. **Handles overlapping descriptors**
   - During splits, descriptors may overlap temporarily
   - Tracks `nextRS` to ensure non-overlapping coverage

---

## Phase 3: Mux RangeFeed - Connection Multiplexing

### Why Mux RangeFeed?

The [`rangefeedMuxer`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L33-L56) provides critical optimizations:

- **One RPC stream per node** instead of per-range
- **Reduced connection overhead** for many ranges
- **Multiplexed events** with stream IDs to demux on client

```go
type rangefeedMuxer struct {
	g ctxgroup.Group

	ds         *DistSender
	metrics    *DistSenderRangeFeedMetrics
	cfg        rangeFeedConfig
	registry   *rangeFeedRegistry
	catchupSem *catchupScanRateLimiter
	eventCh    chan<- RangeFeedMessage

	seqID int64  // atomic stream ID generator

	muxClients syncutil.Map[roachpb.NodeID, future.Future[muxStreamOrError]]
}
```

### Mux Stream Lifecycle

Each [`muxStream`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L110-L121) to a node is:

- **Bidirectional gRPC stream**
  - `sender`: Client sends `RangeFeedRequest` messages
  - `receiver`: Server sends `MuxRangeFeedEvent` messages
- **Multiplexes many logical rangefeeds**
  - Each has unique `StreamID`
  - Events tagged with `StreamID` for demultiplexing
- **Fails independently**
  - Logical rangefeed errors don't tear down stream
  - Stream errors require all rangefeeds to restart

### Starting a Single Rangefeed

At [`startSingleRangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L202-L239), the muxer:

1. **Creates [`activeMuxRangeFeed`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L158-L168)** structure at line 215
   - Wraps [`activeRangeFeed`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L308-L333) with transport state
   - Tracks routing, replica, timestamp info

2. **Acquires catch-up scan quota** at line 256 via [`acquireCatchupScanQuota()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L594-L615)
   - Rate limits concurrent catch-up scans (client-side)
   - Prevents overwhelming the cluster
   - Released after first checkpoint received

3. **Establishes transport** at lines 276-283
   - Calls [`newTransportForRange()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L617-L629)
   - Builds replica list, optimizes replica order
   - Uses `RangefeedClass` connection (separate from normal traffic)

4. **Gets or creates mux stream** to target node
   - Retrieves existing stream from `muxClients` cache
   - Or establishes new `MuxRangeFeed` RPC

5. **Sends [`RangeFeedRequest`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L631-L670)** at line 286
   - Constructed via [`makeRangeFeedRequest()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L635)
   - Includes span, rangeID, timestamp, options
   - Tagged with unique `StreamID`

### Event Reception

A dedicated goroutine per node runs [`receiveEventsFromNode()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L366), continuously receiving [`MuxRangeFeedEvent`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvpb/api.proto) messages and:

1. Looking up `activeMuxRangeFeed` by `StreamID`
2. Calling `onRangeEvent()` to update tracking state
3. Forwarding event to client's `eventCh`
4. Handling errors via [`handleRangefeedError()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L518-L584)

---

## Phase 4: Server-Side Processing

### Request Arrival at Replica

The gRPC request arrives at [`Replica.RangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L223-L339):

```go
func (r *Replica) RangeFeed(
	streamCtx context.Context,
	args *kvpb.RangeFeedRequest,
	stream rangefeed.Stream,
	pacer *admission.Pacer,
	perConsumerCatchupLimiter *limit.ConcurrentRequestLimiter,
) (rangefeed.Disconnector, error)
```

This method performs critical validation at lines 230-261:

1. **Converts span to RSpan** (line 232)
2. **Ensures closed timestamp updates started** (line 237)
3. **Checks GC threshold** (line 256)
   - If catch-up scan requested (`!args.Timestamp.IsEmpty()`)
   - Timestamp must be >= GCThreshold
   - Otherwise returns `BatchTimestampBeforeGCError`

4. **Acquires catch-up iterator quota** (lines 263-300)
   - Server-side rate limiting separate from client-side
   - Uses `r.store.limiters.ConcurrentRangefeedIters`
   - Per-consumer limiter prevents single consumer overload

### Registration Under RaftMu

**Critical**: Registration happens under [`raftMu`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L306) (line 306) to ensure atomicity:

- Catch-up iterator snapshot is captured
- Registration is established
- **No events are missed** between snapshot and registration

At line 329, calls [`registerWithRangefeedRaftMuLocked()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L430-L556).

### Rangefeed Processor Creation

The processor is the heart of server-side rangefeed logic. At lines 488-508:

```go
cfg := rangefeed.Config{
	AmbientContext:   r.AmbientContext,
	Clock:            r.Clock(),
	Stopper:          r.store.stopper,
	Settings:         r.store.ClusterSettings(),
	RangeID:          r.RangeID,
	Span:             desc.RSpan(),
	TxnPusher:        &tp,
	PushTxnsAge:      r.store.TestingKnobs().RangeFeedPushTxnsAge,
	EventChanCap:     kvserverbase.DefaultRangefeedEventCap,
	EventChanTimeout: defaultEventChanTimeout,
	Metrics:          r.store.metrics.RangeFeedMetrics,
	MemBudget:        feedBudget,
	Scheduler:        r.store.getRangefeedScheduler(),
	Priority:         isSystemSpan,
}
p = rangefeed.NewProcessor(cfg)
```

[`NewProcessor()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L242-L246) creates a [`ScheduledProcessor`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/scheduled_processor.go) which uses a shared worker pool instead of dedicated goroutines per rangefeed.

### Resolved Timestamp Initialization

At lines 511-518, an [`IntentScannerConstructor`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L152) is provided:

```go
rtsIter := func() (rangefeed.IntentScanner, error) {
	r.raftMu.AssertHeld()
	return rangefeed.NewSeparatedIntentScanner(streamCtx, r.store.TODOEngine(), desc.RSpan())
}
```

When [`p.Start()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L525) is called, the processor:
1. Scans for unresolved intents
2. Initializes resolved timestamp to safe value
3. Begins accepting registrations

### Stream Registration

At line 534, calls [`p.Register()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L190-L200):

```go
reg, disconnector, filter := p.Register(streamCtx, span, startTS, catchUpSnap, withDiff,
	withFiltering, withOmitRemote, bulkDeliverySize, stream)
```

This creates a [`registration`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/registry.go) in the processor's registry and:

1. **Initiates catch-up scan** if `catchUpSnap != nil`
   - Scans engine from `startTS` (exclusive) to present
   - Streams all MVCC versions as `RangeFeedValue` events
   - Releases iterator quota when complete

2. **Returns [`Filter`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/filter.go)**
   - Tracks which operations need values
   - Optimizes value lookups during event generation

3. **Registers stream with buffered sender**
   - Events queued in memory budget
   - Send timeout prevents slow consumers blocking writers

---

## Phase 5: Event Generation from Raft Log

### Logical Operations Flow

When Raft commands apply, the replica calls [`handleLogicalOpLogRaftMuLocked()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L671-L802) at replica_rangefeed.go:678:

```go
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *kvserverpb.LogicalOpLog, batch storage.Batch,
)
```

This method is **critical** - it transforms Raft log entries into rangefeed events.

### Logical Op Log Structure

The [`LogicalOpLog`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/kvserverpb/proposer_kv.proto) contains:

```proto
message LogicalOpLog {
  repeated enginepb.MVCCLogicalOp ops = 1;
}
```

Each [`MVCCLogicalOp`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/storage/enginepb/mvcc3.proto) can be:
- `MVCCWriteValueOp` - Direct write (1PC txn)
- `MVCCCommitIntentOp` - Intent commit
- `MVCCWriteIntentOp` - Intent creation
- `MVCCUpdateIntentOp` - Intent update
- `MVCCAbortIntentOp` - Intent abort
- `MVCCDeleteRangeOp` - Range tombstone

### Value Population

Lines 716-795 iterate operations and populate missing fields:

1. **Read values from batch** (line 781)
   - Uses [`MVCCGetForKnownTimestampWithNoIntent()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L781)
   - Batch is **indexed** allowing efficient lookups
   - Values must exist (same critical section as apply)

2. **Respects filter** (line 774)
   - `filter.NeedVal()` checks if any registration needs this key
   - Skips value lookup if no registration interested
   - Still tracks intent changes for resolved timestamp

3. **Populates metadata** (lines 792-794)
   - `OmitInRangefeeds` flag (for filtered writes)
   - `OriginID` (for LDR/multi-region)

### Consumption by Processor

Line 798 calls [`p.ConsumeLogicalOps()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L217):

```go
if !p.ConsumeLogicalOps(ctx, ops.Ops...) {
	// Consumption failed and the rangefeed was stopped.
	r.unsetRangefeedProcessor(p)
}
```

The processor:
1. **Sends ops to internal event channel** with timeout
   - If timeout exceeded → disconnects slow consumers
   - Returns `false` if processor stopped

2. **Transforms ops to [`RangeFeedEvent`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvpb/api.proto)**
   - `MVCCWriteValueOp` → `RangeFeedValue`
   - `MVCCCommitIntentOp` → `RangeFeedValue`
   - Intent ops → Update unresolved intent tracking

3. **Publishes to registrations**
   - Registry's `PublishToOverlapping()` finds matching registrations
   - Respects `withFiltering` flag
   - Applies diff mode if requested

### SSTable Ingestion

For `AddSSTable` operations, [`handleSSTableRaftMuLocked()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L804-L824) at line 814:

- Calls [`p.ConsumeSSTable()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L219-L225)
- Sends entire SSTable as [`RangeFeedSSTable`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvpb/api.proto) event
- **Warning**: No memory budgeting currently (issue #73616)

---

## Phase 6: Closed Timestamp and Resolved Timestamps

### Closed Timestamp Updates

The [`handleClosedTimestampUpdateRaftMuLocked()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L843-L932) method at line 845:

```go
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(
	ctx context.Context, closedTS hlc.Timestamp,
) (exceedsSlowLagThresh bool)
```

Called periodically (every [`kv.rangefeed.closed_timestamp_refresh_interval`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L51-L60), default 3s):

1. **Checks lag** via `rangefeedCTLagObserver` (line 857)
   - If lag > 5x target → nudges leaseholder
   - If lag persistently high → cancels rangefeed for replanning

2. **Forwards to processor** (line 926)
   - Calls [`p.ForwardClosedTS()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L226-L230)
   - Processor updates internal closed timestamp

### Resolved Timestamp Emission

The processor maintains a [`resolvedTimestamp`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/resolved_timestamp.go) that:

1. **Tracks unresolved intents**
   - Each intent write → added to `unresolvedIntentQueue`
   - Intent resolution → removed from queue

2. **Computes resolved timestamp**
   - `min(closedTimestamp, oldestUnresolvedIntent.Timestamp - 1)`
   - Guarantees: no writes below resolved TS will appear

3. **Emits checkpoints**
   - [`RangeFeedCheckpoint`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvpb/api.proto) events
   - Per-span resolved timestamps
   - Clients track frontier across all spans

### Transaction Pushing

When intents block resolved timestamp progress, the [`rangefeedTxnPusher`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L110-L210):

1. **Pushes old transactions** (line 121)
   - Age threshold: [`kv.rangefeed.push_txns.enabled`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L36-L43) (default 10s)
   - High-priority push to advance write timestamp

2. **Resolves intents** (line 154)
   - After successful push → resolve intents
   - Uses `NormalPri` admission priority (timely delivery)

3. **Issues barriers** (line 173)
   - After ambiguous txn abort
   - Ensures all prior writes applied before checkpoint

---

## Phase 7: Event Delivery to Client

### Stream Multiplexing

Events flow back through the mux stream:

1. **Server-side**: Processor writes to `Stream` interface
   - Backed by gRPC bidirectional stream
   - Tagged with `StreamID`

2. **Network**: `MuxRangeFeedEvent` sent over gRPC

3. **Client-side**: [`receiveEventsFromNode()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L366) demultiplexes
   - Looks up `activeMuxRangeFeed` by `StreamID`
   - Sends to client's `eventCh`

### Event Processing

Back in the client's [`processEvents()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L432-L446) loop at line 433:

```go
func (f *RangeFeed) processEvents(
	ctx context.Context, frontier span.Frontier, eventCh <-chan kvcoord.RangeFeedMessage,
) error
```

Continuously receives events and calls [`processEvent()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L448-L530) which dispatches:

### Event Type Handling

At lines 451-527, events are handled by type:

**Values** (line 452):
```go
case ev.Val != nil:
	f.onValue(ctx, ev.Val)
```
- Calls user-provided `OnValue` callback
- Invoked synchronously in rangefeed goroutine

**Checkpoints** (line 454):
```go
case ev.Checkpoint != nil:
	ts := ev.Checkpoint.ResolvedTS
	advanced, err := frontier.Forward(ev.Checkpoint.Span, ts)
	if advanced && f.onFrontierAdvance != nil {
		f.onFrontierAdvance(ctx, frontier.Frontier())
	}
```
- Updates span frontier
- Calls `onFrontierAdvance` when overall frontier advances
- Frontier represents minimum resolved TS across all spans

**SSTables** (line 473):
```go
case ev.SST != nil:
	f.onSSTable(ctx, ev.SST, registeredSpan)
```
- Delivers entire SSTable for bulk ingestion
- Used by changefeeds for large imports/restores

**DeleteRange** (line 479):
```go
case ev.DeleteRange != nil:
	f.onDeleteRange(ctx, ev.DeleteRange)
```
- MVCC range tombstone notification
- Requires explicit handler registration

**BulkEvents** (line 496):
- Batch optimization for catch-up scans
- Reduces per-event overhead
- Can contain mix of value/checkpoint/SST events

### Frontier Tracking

The [`span.Frontier`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/util/span/frontier.go) maintains:
- Minimum resolved timestamp per span
- Overall frontier = min across all spans
- Compact representation using interval tree

---

## Phase 8: Error Handling and Retries

### Error Classification

Errors are classified by [`handleRangefeedError()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L518-L584) at dist_sender_rangefeed.go:521:

**Retryable without eviction** (retry same replica):
- `io.EOF` - clean stream shutdown
- `REASON_REPLICA_REMOVED` - replica moved
- `REASON_RAFT_SNAPSHOT` - replica caught up via snapshot
- `REASON_LOGICAL_OPS_MISSING` - old raft entries (pre-rangefeed)
- `REASON_SLOW_CONSUMER` - backpressure limit hit
- `REASON_RANGEFEED_CLOSED` - graceful shutdown

**Requires eviction** (refresh routing, try different replica):
- `StoreNotFoundError` - stale descriptor
- `NodeUnavailableError` - node down
- `RangeNotFoundError` - range moved/merged
- Send errors (network)

**Requires span re-resolution** (range boundaries changed):
- `RangeKeyMismatchError` - descriptor stale
- `REASON_RANGE_SPLIT` - range split occurred
- `REASON_RANGE_MERGED` - range merged
- `REASON_NO_LEASEHOLDER` - lease moved

**Unrecoverable** (propagate to client):
- `BatchTimestampBeforeGCError` - GC threshold exceeded
- `MVCCHistoryMutationError` - MVCC history changed
- Auth errors
- Context cancellation

### Retry Mechanics

The retry loop at [`activeMuxRangeFeed.start()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L252-L364) (line 261):

1. **Transient errors**: Retry with backoff
2. **Eviction errors**: Clear `token`, re-lookup descriptor
3. **Span resolution errors**: Call `divideSpanOnRangeBoundaries()` again
4. **Fatal errors**: Return to client, terminate rangefeed

### Client-Side Retry

The client's [`run()` loop](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L307-L430) at line 360:

```go
for i := 0; r.Next(); i++ {
	ts := frontier.Frontier()

	err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)

	if errors.HasType(err, (*kvpb.BatchTimestampBeforeGCError)(nil)) ||
	   errors.HasType(err, (*kvpb.MVCCHistoryMutationError)(nil)) {
		if errCallback := f.onUnrecoverableError; errCallback != nil {
			errCallback(ctx, err)
		}
		return
	}

	// Reset backoff if ran successfully > 30s
	if ranFor > resetThreshold {
		i = 1
		r.Reset()
	}
}
```

---

## Phase 9: Changefeed Integration

Changefeeds are the primary consumer of rangefeeds. Let's trace the integration:

### Changefeed Startup

At [`pkg/ccl/changefeedccl/kvfeed/physical_kv_feed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/ccl/changefeedccl/kvfeed/physical_kv_feed.go), the `rangefeed` type:

```go
type rangefeed struct {
	spans  []roachpb.Span
	withDiff bool
	withFiltering bool
	// ... other fields
}
```

Starts a rangefeed via the standard [`Factory.RangeFeed()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L138-L151) API.

### Event Handling

Events are processed by [`handleRangefeedEvent()`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/ccl/changefeedccl/kvfeed/physical_kv_feed.go#L143):

1. **Values**: Converted to [`kvevent.Event`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/ccl/changefeedccl/kvevent/event.go#L137-L299)
2. **Checkpoints**: Update changefeed frontier
3. **SSTs**: Streamed for bulk loading

### Changefeed-Specific Options

Changefeeds use:
- `WithDiff()` - Previous values for UPDATE events
- `WithFiltering()` - Skip `OmitInRangefeeds` writes
- `WithMatchingOriginIDs()` - LDR origin filtering

---

## Data Flow Summary

Let's trace a single write through the entire rangefeed pipeline:

### 1. Write Execution (Not Shown)
```
Client Write → DistSender → Leaseholder Replica → Raft Proposal
```

### 2. Raft Application
```
Raft Apply → Replica.handleLogicalOpLogRaftMuLocked()
    ↓
  LogicalOpLog with MVCCWriteValueOp
    ↓
  Read value from indexed batch
    ↓
  p.ConsumeLogicalOps([op])
```
[`replica_rangefeed.go:678`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L678)

### 3. Processor Event Generation
```
Processor.ConsumeLogicalOps()
    ↓
  Transform to RangeFeedValue event
    ↓
  Registry.PublishToOverlapping()
    ↓
  For each matching registration:
    → Check filter (withFiltering)
    → Apply diff (withDiff)
    → Send to registration.Stream
```
[`processor.go:217`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L217)

### 4. Network Transmission
```
Registration.Stream (gRPC)
    ↓
  MuxRangeFeedEvent { StreamID, RangeFeedEvent }
    ↓
  Network transmission to client
```

### 5. Client Reception
```
muxStream.receiver.Recv()
    ↓
  receiveEventsFromNode()
    ↓
  Lookup activeMuxRangeFeed by StreamID
    ↓
  Send to client eventCh
```
[`dist_sender_mux_rangefeed.go:366`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go#L366)

### 6. Client Event Processing
```
RangeFeed.processEvents()
    ↓
  processEvent() → dispatch by type
    ↓
  ev.Val != nil → onValue(ctx, ev.Val)
    ↓
  User callback invoked
```
[`rangefeed.go:433`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go#L433)

---

## Performance Considerations

### Memory Management

**Budget Tracking**: [`FeedBudget`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/budget.go)
- System vs non-system rangefeed quotas
- Shared memory pool per store
- Prevents OOM from rangefeed event buffering

**Buffered Sender**: [`kv.rangefeed.buffered_sender.enabled`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L86-L95)
- Node-level event buffering
- More efficient than per-registration channels

### Rate Limiting

**Client-side catch-up**: [`catchupScanRateLimiter`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L710-L727)
- Default: 100 ranges/sec
- Setting: `kv.rangefeed.client.stream_startup_rate`
- Prevents overwhelming cluster during restarts

**Server-side iterators**: `ConcurrentRangefeedIters`
- Limits concurrent catch-up scans per store
- Prevents IO saturation

### Connection Management

**Mux Streams**: One RPC per node
- Dramatically reduces connection count
- Scales to tens of thousands of ranges
- Lower overhead than legacy per-range RPCs

**Connection Class**: `RangefeedClass`
- Separate connection pool from foreground traffic
- Can be disabled via `COCKROACH_RANGEFEED_USE_DEFAULT_CONNECTION_CLASS`

---

## Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| [`kv.rangefeed.enabled`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L40-L49) | `false` | Global rangefeed enablement |
| [`kv.rangefeed.closed_timestamp_refresh_interval`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L51-L60) | `3s` | Checkpoint emission frequency |
| [`kv.rangefeed.client.stream_startup_rate`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go#L48-L55) | `100/s` | Client-side catch-up rate limit |
| [`kv.rangefeed.push_txns.enabled`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go#L36-L43) | `true` | Push old txns to advance resolved TS |
| [`kv.rangefeed.bulk_delivery.size`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go#L212-L217) | `2MB` | Bulk event batch size |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLIENT APPLICATION                          │
│                                                                     │
│  Factory.RangeFeed(spans, onValue) → RangeFeed                     │
│      │                                                              │
│      ├─► runInitialScan() (optional)                               │
│      └─► run() loop with retry                                     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DISTSENDER LAYER                            │
│                                                                     │
│  RangeFeed() → divideAllSpansOnRangeBoundaries()                   │
│      │                                                              │
│      ├─► For each range: startSingleRangeFeed()                    │
│      │                                                              │
│      └─► rangefeedMuxer - one stream per node                      │
│             │                                                       │
│             ├─► activeMuxRangeFeed (per range)                     │
│             │     - Routing & transport                            │
│             │     - Catchup quota                                  │
│             │     - Retry state                                    │
│             │                                                       │
│             └─► muxStream (per node)                               │
│                   - sender.Send(RangeFeedRequest)                  │
│                   - receiver.Recv(MuxRangeFeedEvent)               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ gRPC MuxRangeFeed RPC
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         KVSERVER (REPLICA)                          │
│                                                                     │
│  Replica.RangeFeed(args, stream)                                   │
│      │                                                              │
│      ├─► Check GC threshold                                        │
│      ├─► Acquire iterator quota                                    │
│      ├─► Lock raftMu                                               │
│      │                                                              │
│      └─► registerWithRangefeedRaftMuLocked()                       │
│             │                                                       │
│             ├─► Create/reuse Processor                             │
│             │                                                       │
│             └─► p.Register(span, stream)                           │
│                   - Run catchup scan                               │
│                   - Add to registry                                │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     RANGEFEED PROCESSOR                             │
│                                                                     │
│  Processor (per range, multiple registrations)                     │
│      │                                                              │
│      ├─► resolvedTimestamp                                         │
│      │     - Track unresolved intents                              │
│      │     - Compute min(closedTS, oldestIntent-1)                 │
│      │     - Emit checkpoints                                      │
│      │                                                              │
│      ├─► Registry                                                  │
│      │     - PublishToOverlapping(event, span)                     │
│      │     - Apply filtering, diff mode                            │
│      │     - Send to Stream                                        │
│      │                                                              │
│      └─► Event consumption:                                        │
│            - ConsumeLogicalOps()                                   │
│            - ConsumeSSTable()                                      │
│            - ForwardClosedTS()                                     │
└──────────────────────────┴──────────────────────────────────────────┘
                           ▲
                           │
    ┌──────────────────────┴──────────────────────┐
    │                                             │
    │  Raft Log Application                       │
    │                                             │
    │  handleLogicalOpLogRaftMuLocked()           │
    │    - Read values from batch                 │
    │    - Populate LogicalOpLog                  │
    │    - p.ConsumeLogicalOps()                  │
    │                                             │
    │  handleClosedTimestampUpdateRaftMuLocked()  │
    │    - p.ForwardClosedTS()                    │
    │                                             │
    └─────────────────────────────────────────────┘
```

---

## Component File Map

### Client-Side

| Component | File | Key Types/Functions |
|-----------|------|---------------------|
| Entry Point | [`pkg/kv/kvclient/rangefeed/rangefeed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/rangefeed/rangefeed.go) | `Factory`, `RangeFeed`, `Start()`, `run()` |
| DistSender | [`pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_rangefeed.go) | `RangeFeed()`, `divideAllSpansOnRangeBoundaries()` |
| Mux Rangefeed | [`pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvclient/kvcoord/dist_sender_mux_rangefeed.go) | `rangefeedMuxer`, `muxStream`, `activeMuxRangeFeed` |

### Server-Side

| Component | File | Key Types/Functions |
|-----------|------|---------------------|
| Replica Entry | [`pkg/kv/kvserver/replica_rangefeed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/replica_rangefeed.go) | `RangeFeed()`, `registerWithRangefeedRaftMuLocked()`, `handleLogicalOpLogRaftMuLocked()` |
| Processor | [`pkg/kv/kvserver/rangefeed/processor.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/processor.go) | `Processor` interface, `NewProcessor()` |
| Scheduled Processor | [`pkg/kv/kvserver/rangefeed/scheduled_processor.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/scheduled_processor.go) | `ScheduledProcessor` |
| Registry | [`pkg/kv/kvserver/rangefeed/registry.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/registry.go) | `registry`, `registration`, `PublishToOverlapping()` |
| Resolved TS | [`pkg/kv/kvserver/rangefeed/resolved_timestamp.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/resolved_timestamp.go) | `resolvedTimestamp`, `unresolvedIntentQueue` |
| Catchup Scan | [`pkg/kv/kvserver/rangefeed/catchup_scan.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/kv/kvserver/rangefeed/catchup_scan.go) | `CatchUpSnapshot` |

### Changefeed Integration

| Component | File | Key Types/Functions |
|-----------|------|---------------------|
| Physical Feed | [`pkg/ccl/changefeedccl/kvfeed/physical_kv_feed.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/ccl/changefeedccl/kvfeed/physical_kv_feed.go) | `rangefeed`, `handleRangefeedEvent()` |
| Event Wrapper | [`pkg/ccl/changefeedccl/kvevent/event.go`](https://github.com/cockroachdb/cockroach/blob/8385dd71290393b5bc5ec4f3f9d2dab183743c26/pkg/ccl/changefeedccl/kvevent/event.go) | `Event`, `MakeKVEvent()` |

---

## Statistics

**Files analyzed**: 12 core rangefeed files
**Code links generated**: 85+
**Trace depth**: 7 architectural layers
**Total flow span**: From client application → Raft log → back to client

---

## Conclusion

A rangefeed in CockroachDB is a sophisticated distributed streaming mechanism that:

1. **Scales efficiently** via mux streams and shared processor pools
2. **Guarantees consistency** through resolved timestamps and closed timestamps
3. **Handles failures gracefully** with automatic retry and error classification
4. **Provides rich semantics** including diffs, filtering, and bulk delivery
5. **Integrates deeply** with Raft, MVCC, and transaction systems

The architecture demonstrates CockroachDB's commitment to:
- **Correctness**: Atomic registration under raftMu, fence-post timestamp handling
- **Performance**: Memory budgeting, rate limiting, connection multiplexing
- **Operability**: Comprehensive error handling, observable metrics, adaptive retry

This "Life of a Rangefeed" trace provides a foundation for understanding change data capture, real-time notifications, and eventually-consistent caching in distributed SQL systems.
