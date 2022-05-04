package mongosync

import (
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"time"
)

func TestGetCollChan(t *testing.T) {
	bwChan := make(chan *BulkWriteOp)
	putBwOp := func(bwOp *BulkWriteOp) *sync.WaitGroup {
		bwChan <- bwOp
		return nil
	}
	const maxDelay = 200
	const maxBatch = 3
	ch := getCollChan("testcol", maxDelay, maxBatch, true, putBwOp)
	waitResult := func(delay int) (received bool, bwOp *BulkWriteOp) {
		// ch is empty, should expire 100ms with no input
		select {
		case <-time.After(time.Millisecond * time.Duration(delay)):
		case bwOp = <-bwChan:
			received = true
		}
		return
	}
	received, bwOp := waitResult(100)
	require.False(t, received)
	require.Nil(t, bwOp)
	// now put simple drop op to channel but wait too few
	opDrop := createOp(t, bson.M{"operationType": "drop"})
	ch <- opDrop
	received, bwOp = waitResult(100)
	require.True(t, received) // opDrop flushed immediately
	require.NotNil(t, bwOp)
	require.True(t, bwOp.RealTime)
	require.Equal(t, "testcol", bwOp.Coll)
	require.Equal(t, "", bwOp.SyncId)
	require.Equal(t, OpLogDrop, bwOp.OpType)
	// now opInsert
	opInsert := createOp(t, bson.M{"operationType": "insert", "fullDocument": bson.M{"name": "test"}})
	ch <- opInsert
	// wait to few so no flush
	received, bwOp = waitResult(50)
	require.False(t, received)
	require.Nil(t, bwOp)
	// now wait more, should receive it
	received, bwOp = waitResult(300)
	require.True(t, received) // opDrop flushed immediately
	require.NotNil(t, bwOp)
	require.True(t, bwOp.RealTime)
	require.Equal(t, "testcol", bwOp.Coll)
	require.Equal(t, "", bwOp.SyncId)
	require.Equal(t, OpLogUnordered, bwOp.OpType)
	// now wait few but add multiple (more then maxBatch)
	go func() {
		for i := 0; i <= maxBatch; i++ {
			opInsert := createOp(t, bson.M{"operationType": "insert", "fullDocument": bson.M{"name": "test"}})
			ch <- opInsert
		}
	}()
	received, bwOp = waitResult(50)
	require.True(t, received) // opDrop flushed immediately
	require.NotNil(t, bwOp)
	require.True(t, bwOp.RealTime)
	require.Equal(t, "testcol", bwOp.Coll)
	require.Equal(t, maxBatch, len(bwOp.Models))
	require.Equal(t, OpLogUnordered, bwOp.OpType)
	// wait for another too feww
	received, bwOp = waitResult(10)
	require.False(t, received) // wait to few
	require.Nil(t, bwOp)
	// now wait enough
	received, bwOp = waitResult(300)
	require.True(t, received) // opDrop flushed immediately
	require.NotNil(t, bwOp)
	require.True(t, bwOp.RealTime)
	require.Equal(t, "testcol", bwOp.Coll)
	require.Equal(t, 1, len(bwOp.Models))
	require.Equal(t, OpLogUnordered, bwOp.OpType)
}

func createOp(t *testing.T, op bson.M) (r bson.Raw) {
	r, err := bson.Marshal(op)
	require.NoError(t, err)
	return
}
