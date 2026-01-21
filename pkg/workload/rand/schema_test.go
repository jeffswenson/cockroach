package rand

import "testing"

func TestLoadTable(t *testing.T) {
	type testCase struct {
		tableName string
		schema  string 
	}

	// (col0_0 "char" NOT NULL, col0_1 TIMESTAMP NOT NULL, col0_2 BIT(3) NOT NULL, col0_3 DATE NOT NULL, PRIMARY KEY (col0_0 ASC, col0_3 ASC, col0_2))

	testCases := []testCase{
		{
			tableName: "asc_primary_key",


}
