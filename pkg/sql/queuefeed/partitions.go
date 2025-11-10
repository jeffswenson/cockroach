package queuefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type Partition struct {
	// ID is the `partition_id` column in the queue partition table.
	ID   int64
	// Session is the `user_session` and `sql_liveness_session` assigned to this
	// partition.
	Session Session
	// Successor is the `user_session_successor` and
	// `sql_liveness_session_successor` assigned to the partition.
	Successor Session
	// Span is decoded from the `partition_spec` column.
	Span *roachpb.Span
}

func (p *Partition) Empty() bool {
	return p == &Partition{}
}

type Session struct {
	// ConnectionID is the ID of the underlying connection.
	ConnectionID uuid.UUID
	// LivenessID is the session ID for the server. Its used to identify sessions
	// that belong to dead sql servers.
	LivenessID sqlliveness.SessionID
}

type partitionCache struct {
	// partitions is a stale cache of the current state of the partition's
	// table. Any assignment decisions and updates should be made using
	// transactions.
	Partitions  map[int64]Partition

	// sessionIndex is a map of sessions to assigned partitions. It is
	// consistent with the partition cache.
	SessionIndex map[Session]map[int64]struct{}

	// partitionIndex is a map of partition IDs to assigned sessions. It is
	// consistent with the partition cache.
	PartitionIndex map[int64]Session
}

func (p *partitionCache) Refresh(partitions map[int64]Partition) {
	return errors.New("not implemented")
}

func planRegister(session Session, cache partitionCache) (tryClaim Partition, trySecede Partition) {
	// Check to see if there is an an unassigned partition that can be claimed.
	for _, partition := range cache.Partitions {
		if _, assigned := cache.PartitionIndex[partition.ID]; !assigned {
			return partition, Partition{}
		}
	}



	return Partition{}, Partition{}
}

func planAssignment(session Session, caughtUp bool, cache partitionCache) (tryRelease []Partition, tryClaim Partition, trySecede Partition) {
		// Check to see if any of the partitions assigned to the session have a
		// successor. If they do, release them.
		var toRelease []Partition
		for partition := range p.sessionIndex[Session] {
			if !cache.Partitions[partition].Successor.Empty() {
				toRelease = append(toRelease, cache.Partitions[partition])
			}
		}
		if len(toRelease) != 0 {
			return toRelease, Partition{}, Partition{}
		}

		// If we aren't caught up, we should not try to claim any new partitions.
		if !caughtUp {
			return nil, Partition{}, Partition{}
		}

		// Check to see if there is an an unassigned partition that can be claimed.
		for _, partition := range p.mu.partitionCache {
			if _, assigned := p.mu.partitionIndex[partition.ID]; !assigned {
				return nil, partition, Partition{}
			}
		}

		// If we have fewer than partitions / len(sessions), try to claim a
		// partition from a session with too many.
		avgPartitions := len(p.mu.partitionCache) / len(p.mu.assignments)
		if avgPartitions <= len(p.mu.sessionIndex[session]) {
			return nil, Partition{}, Partition{}
		}

		return nil, Partition{}, Partition{}
}
