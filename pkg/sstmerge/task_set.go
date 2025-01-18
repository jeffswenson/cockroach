package sstmerge

type taskId int

type taskSpan struct {
	start taskId
	end   taskId
}

func (t *taskSpan) size() int {
	return int(t.end) - int(t.start)
}

func (t taskSpan) split() (taskSpan, taskSpan) {
	mid := (t.start + t.end) / 2
	return taskSpan{start: t.start, end: mid}, taskSpan{start: mid, end: t.end}
}

func makeTaskSet(taskCount int) taskSet {
	return taskSet{
		unassigned: []taskSpan{{start: 0, end: taskId(taskCount)}},
	}
}

type taskSet struct {
	unassigned []taskSpan
}

// ClaimFirst should be called when a worker claims its first task. It returns
// the taskId to process and true if there are more tasks to claim.
//
// ClaimFirst is distinct from ClaimNext because ClaimFirst will always split
// the largest span in the unassigned set, whereas ClaimNext will assign from
// the same span until it is exhausted.
func (t *taskSet) ClaimFirst() (taskId, bool) {
	if len(t.unassigned) == 0 {
		return 0, false
	}

	// Find the largest span
	largest := 0
	for i := range t.unassigned {
		if t.unassigned[largest].size() < t.unassigned[i].size() {
			largest = i
		}
	}

	largestSpan := t.unassigned[largest]
	if largestSpan.size() == 0 {
		return 0, false
	}
	if largestSpan.size() == 1 {
		t.lockedRemoveSpan(largest)
		return largestSpan.start, true
	}

	left, right := largestSpan.split()
	t.unassigned[largest] = left

	task := right.start
	right.start += 1
	if right.size() != 0 {
		t.insertSpan(right, largest+1)
	}

	return task, true
}

func (t *taskSet) ClaimNext(lastTask taskId) (taskId, bool) {
	next := lastTask + 1

	for i, span := range t.unassigned {
		if span.start != next {
			continue
		}

		span.start += 1

		if span.size() == 0 {
			t.lockedRemoveSpan(i)
			return next, true
		}

		t.unassigned[i] = span
		return next, true
	}

	// If we didn't find the next task in the unassigned set, then we've
	// exhausted the span and need to claim from a different span.
	return t.ClaimFirst()
}

func (t *taskSet) insertSpan(span taskSpan, index int) {
	t.unassigned = append(t.unassigned, taskSpan{})
	copy(t.unassigned[index+1:], t.unassigned[index:])
	t.unassigned[index] = span
}

func (t *taskSet) lockedRemoveSpan(index int) {
	t.unassigned = append(t.unassigned[:index], t.unassigned[index+1:]...)
}
