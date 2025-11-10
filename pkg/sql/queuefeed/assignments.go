package queuefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type Assignment struct {
	// Version is unique per process level and can be used to efficiently detect
	// assignment changes.
	Version    int64
	Session	 	 Session
	// Partitions is the list of partitions assigned to the session. It is sorted
	// by ID.
	Partitions []Partition
}


func (s *Session) Empty() bool {
	return s.ConnectionID == uuid.Nil
}

type PartitionAssignments struct {
	db isql.DB
	mu struct {
		syncutil.Mutex
		cache partitionCache
	}
}

func (p *PartitionAssignments) Start(ctx context.Context) error {
	// TODO poll the partition table and update the partition cache
	return errors.New("not implemented")
}

// RefreshAssignment refreshes the assignment for the given session. It returns
// nil if the assignment has not changed.
//
// If the session is caught up (i.e. it has proceessed up to a recent timestamp
// for all assigned partitions), then it may be assigned new partitions.
//
// If a partition has a successor session, then calling RefreshAssignment will
// return an assignment that does not include that partition.
func (p *PartitionAssignments) RefreshAssignment(assignment *Assignment, caughtUp bool) (updatedAssignment *Assignment, err error) {
	dirty, tryRelease, tryClaim, trySecede := func() (bool, []Partition, Partition, Partition) {
		p.mu.Lock()
		defer p.mu.Unlock()
		cache := p.mu.cache

		// Check to see if the assignment is stale. If it is we need to refresh the
		// cache and rebuild the assignment.
		if len(assignment.Partitions) != len(cache.SessionIndex[assignment.Session]) {
			return true, nil, Partition{}, Partition{}
		}
		for _, partition := range assignment.Partitions {
			_, ok := cache.SessionIndex[assignment.Session][partition.ID]
			if !ok {
				return true, nil, Partition{}, Partition{}
			}
		}

		tryRelase, tryClaim, trySecede := planAssignment(assignment.Session, caughtUp, p.mu.cache)
		return false, tryRelase, tryClaim, trySecede
	}()

	if dirty {
		if err := p.refreshCache(); err != nil {
			return nil, err
		}
	} else if tryRelease != nil {

	} else if !tryClaim.Empty() {

	} else if !trySecede.Empty() {

	} else {
		return nil, nil
	}

	return p.constructAssignment(assignment.Session)
}

// RegisterSession registers a new session. The session may be assigned zero
// partitions if there are no unassigned partitions. If it is assigned no
// partitions, the caller can periodically call RefreshAssignment claim
// partitions if they become available.
func (p *PartitionAssignments) RegisterSession(ctx context.Context, session Session) (*Assignment, error) {
	tryClaim, try := func() Partition {
		p.mu.Lock()
		defer p.mu.Unlock()

		for _, partition := range p.mu.cache.Partitions {
			if _, assigned := p.mu.cache.PartitionIndex[partition.ID]; !assigned {
				return partition
			}
		}

		return Partition{}
	}()

	err := p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

	})
	if err != nil {
		return nil, err
	}
	for {
		// Transactionally read the current assignments

		// Loop over the partitions:
		// 1. Sample a random unassigned partition
		// 2. Collect partitions assigned to the session
		// 3. Collect partitions w/ the sessions as a successor

		// If there is an unassigned partition, assign it to the session and clear
		// out successor session claims.
	}
}

func (p *PartitionAssignments) UnregisterSession(session Session) error {
	// Statment:
	// For each partition assigned to the session, set the session to the
	// successor
	return nil
}

func (p *PartitionAssignments) refreshCache() error {
	return nil
}

func (p *PartitionAssignments) constructAssignment(session Session) (*Assignment, error) {
	// Build an assignment for the given session from the partition cache.
	return nil, errors.New("not implemented")
}

func (p *PartitionAssignments) tryClaim(session Session, partition *Partition) (Partition, error) {
	// Try to claim an unassigned partition for the given session.
	return Partition{}, nil
}

func (p *PartitionAssignments) tryRelease(session Session, toRelease []Partition) error {
	// Release the given partitions from the session.
	return nil
}
