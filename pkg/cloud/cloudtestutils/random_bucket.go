package cloudtestutils

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func RandomBucketName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.NewV4())
}
