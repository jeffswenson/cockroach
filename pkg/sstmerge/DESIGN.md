# High Level Algorithm for Import

1. Write out SST to ingest and collect a key sample for each SST. 
2. Use the key samples to determine output SSTs.
3. Distribute merge tasks based on output SSTs. Prefer to merge consecutive
   SSTs on the same node so that the merge can benefit from input SST block
   caching.

# Integration with CRDB

The job SstIngestJob coordinates the process. SST ingest accepts a set of SSTs
of input data and a set of split keys.

SstIngestJob is implemented as two seperate distsql flows. The first flow is the
`SstMerge` flow and is responsible for merging SSTs. It accepts the job input
and produces a list of sorted SSTs. The second flow accepts the list of sorted
SSTs, generates splits within the KV layer, then ingests the SSTs into the KV
layer.

# Ordered Scheduling vs Straggler

We want to schedule merges that are adjacent in keyspace on a node so they can
benefit from a cache of file handles and sst blocks, but we don't want to get
stuck waiting for a straggler to complete. This means we don't want to use a
single distsql flow for each task (since we wouldn't preserve the cache between
requests), but we also don't want dispatch all tasks in a single flight since
nodes may end up with an uneven distribution of tasks.

We could rely on a sql server wide cache for these objects, but we want to tie
the lifetime of them to the job. If the job ends up in a paused or failed
state, the caches need to flush.

If there is a way to inject rows into a distsql flow, we could use that to feed
rows to the processor, then the processor would output a row when the flow is
complete.

David has a suggestion for how to use flows to schedule tasks:
1. Create a node that is the workload coordinator. The workload coordinator
   emits rows for each task.
2. Create nodes that are the data processors. The data processors consume the
   rows, perform the work, then emit a row when they are finished.
3. Create a node that is the completion tracker. It accepts the completion
   notification rows.
4. Use DistSQL to place the workload coordinator and the completion tracker on
   the same node. The completion tracker registers a channel with an in-memory
   map and the channel is retrieved by the work coordinator.

# Admission Control

We need to control the rate of work, but also the amount of memory we are
using. There may be multiple concurrent imports. We should prioritize the
completion of one import where possible.

# Simple Implementation

1. Distribute input shards per-node.
2. Have each node merge all input shards.
3. While producing per-node output SSTs, produce key samples for each SST.
4. Collect key samples on the coordinator to pick output splits.
5. Distribute merge tasks based on output splits.
