package tenantcostserver

import "github.com/cockroachdb/errors"

type batchCostServer struct {
}

type requestBatch struct {
	
}

func (b *batchCostServer) applyBatch(r *requestBatch) error {
	// TODO start a high priority transaction

	// TODO list all instances

	// TODO update ru consumption values per the requests

	// TODO upsert records

	// TODO delete old records
	return errors.New("batchCostServer is not implemented")
}
