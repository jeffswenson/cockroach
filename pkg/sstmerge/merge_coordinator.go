package sstmerge

import "sync"

// Strategy
//
// Implement merge as a local parallel process, then convert so the parallel
// workers are a distsql flow.
//
// Each worker will independently checkpoint their status. The checkpoints are
// collected when re-planning to reconstruct which spans need to be assigned.
//
// The point of this strategy is to minimize the amount of work done by the
// coordinator. All it needs to do is hand out new tasks.
//
// When asked to replan, we can have workers checkpoint after their task is
// complete.
//
// Each worker will request tasks from the distsql processor.

// coordinator is initialized with the set of splits and ssts.
// it initializes the work queue and creates the first tasks to hand out.
// workers indicate call ClaimNext to get the next work item.

type spanState int

var (
	spanStateUnassigned spanState = 0
	spanStateAssigned   spanState = 1
	spanStateCompleted  spanState = 2
)

type mergeCoordinator struct {
	sync.Mutex
	tasks taskSet
}

// ClaimFirst should be called when a worker claims its first task. It returns
// the taskId to process and true if there are more tasks to claim.
//
// ClaimFirst is distinct from ClaimNext because ClaimFirst will always split
// the largest span in the unassigned set, whereas ClaimNext will assign from
// the same span until it is exhausted.
func (c *mergeCoordinator) ClaimFirst() (taskId, bool) {
	c.Lock()
	defer c.Unlock()
	return c.tasks.ClaimFirst()
}

func (c *mergeCoordinator) ClaimNext(lastTask taskId) (taskId, bool) {
	c.Lock()
	defer c.Unlock()
	return c.tasks.ClaimNext(lastTask)
}
