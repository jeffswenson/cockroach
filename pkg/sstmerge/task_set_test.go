package sstmerge

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskSetSingleWorker(t *testing.T) {
	// When a single worker claims tasks from a taskSet, it should claim all
	// tasks in order.
	tasks := makeTaskSet(10)
	var found []taskId

	for next, ok := tasks.ClaimFirst(); ok; next, ok = tasks.ClaimNext(next) {
		found = append(found, next)
	}

	// The tasks are claimed in a weird order because it assumes the first span
	// is already claimed. It's possible to get a smoother task distribution by
	// allocating tasks and splitting the spans when initializing the workers.
	require.Equal(t, []taskId{
		5, 6, 7, 8, 9,
		2, 3, 4,
		1,
		0,
	}, found)
}

func TestTaskSetParallel(t *testing.T) {
	taskCount := min(rand.Intn(10000), 16)
	tasks := makeTaskSet(taskCount)
	workers := make([]taskId, 16)
	var found []taskId

	for i := range workers {
		var ok bool
		workers[i], ok = tasks.ClaimFirst()
		found = append(found, workers[i])
		require.True(t, ok)
	}

	for {
		// pick a random worker to claim the next task
		workerIndex := rand.Intn(len(workers))
		prevTask := workers[workerIndex]
		next, ok := tasks.ClaimNext(prevTask)
		if !ok {
			break
		}
		workers[workerIndex] = next
		found = append(found, next)
	}

	// build a map of the found tasks to ensure they are unique
	taskMap := make(map[taskId]struct{})
	for _, task := range found {
		taskMap[task] = struct{}{}
	}
	require.Len(t, taskMap, taskCount)
}
