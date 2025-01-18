package sstmerge

type workItem struct {
	id       int
	Start    []byte
	End      []byte
	Assigned bool
}

type workItemID int

type workQueue struct {
	items []workItem
}

func (w workQueue) ClaimNext(finished workItem) (workItem, bool) {
	return workItem{}, false
}
