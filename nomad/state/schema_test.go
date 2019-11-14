package state

import (
	"testing"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/stretchr/testify/require"
)

func TestStateStoreSchema(t *testing.T) {
	schema := stateStoreSchema()
	_, err := memdb.NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestState_singleRecord(t *testing.T) {
	require := require.New(t)

	db, err := memdb.NewMemDB(&memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"example": {
				Name: "example",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:         "id",
						AllowMissing: false,
						Unique:       true,
						Indexer:      singleRecord,
					},
				},
			},
		},
	})
	require.NoError(err)

	count := func() int {
		txn := db.Txn(false)
		defer txn.Abort()

		iter, err := txn.Get("example", "id")
		require.NoError(err)

		num := 0
		for item := iter.Next(); item != nil; item = iter.Next() {
			num++
		}
		return num
	}

	insert := func(s string) {
		txn := db.Txn(true)
		err := txn.Insert("example", s)
		require.NoError(err)
		txn.Commit()
	}

	insert("one")
	require.Equal(1, count())

	insert("two")
	require.Equal(1, count())

	insert("three")
	require.Equal(1, count())
}
