package mongosync

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"yadex/config"
)

var ExchCfg = &config.ExchangeConfig{
	SenderURI:   "mongodb://localhost:27021",
	SenderDB:    "test",
	ReceiverURI: "mongodb://localhost:27023",
	ReceiverDB:  "test",
}

func CreateDocs(start, numRecs int64) []interface{} {
	docs := make([]interface{}, numRecs)
	for i := start; i < start+numRecs; i++ {
		docs[i-start] = bson.D{{"_id", i}}
	}
	return docs
}

func TestNewMongoSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	av := <-ms.ExchangeAvailable
	require.True(t, av)
	cancel()
	ms.WaitDone.Wait()
}

// TestSyncCollection simple test which
// 1. creates collection of 1000 docs at sender
// 2. copies it to the receiver using syncCollection routine
// 3. checks 1000 records delivered
func TestSyncCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	const collName = "test"
	const numDocs = int64(1000)
	av := <-ms.ExchangeAvailable
	require.True(t, av)
	coll := ms.Sender.Collection(collName)
	err = coll.Drop(ctx)
	require.NoError(t, err)
	res, err := coll.InsertMany(ctx, CreateDocs(1, numDocs))
	require.NoError(t, err)
	require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
	//mIds := Ids2Map(res.InsertedIDs)
	var wg sync.WaitGroup
	putOp := func(bwOp *BulkWriteOp) *sync.WaitGroup {
		bwr, err := ms.Receiver.Collection(bwOp.Coll).BulkWrite(ctx, bwOp.Models)
		require.NoError(t, err)
		require.Equal(t, int64(len(bwOp.Models)), bwr.InsertedCount)
		return &wg
	}
	err = syncCollection(ctx, ms.Sender, ms.Receiver, "test", 100, putOp)
	require.NoError(t, err)
	cancel()
	ms.WaitDone.Wait()
}

// TestSyncCollection2Steps test
// 1. insert 1000 records
// 2. sync to the receiver
// 3. insert another 1000 records
// 4. sync to the receiver. This time it should be 1000 records copied, not 2000
func TestSyncCollectionMultiple(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	const collName = "test"
	const numDocs = int64(100000)
	av := <-ms.ExchangeAvailable
	require.True(t, av)
	coll := ms.Sender.Collection(collName)
	err = coll.Drop(ctx)
	require.NoError(t, err)
	//mIds := Ids2Map(res.InsertedIDs)
	var wg sync.WaitGroup
	receiverColl := ms.Receiver.Collection(collName)
	err = receiverColl.Drop(ctx)
	require.NoError(t, err)
	putOp := func(bwOp *BulkWriteOp) *sync.WaitGroup {
		bwr, err := ms.Receiver.Collection(bwOp.Coll).BulkWrite(ctx, bwOp.Models)
		require.NoError(t, err)
		require.Equal(t, int64(len(bwOp.Models)), bwr.InsertedCount)
		return &wg
	}

	for i := int64(0); i <= 3; i++ {
		// insert another 1000
		res, err := coll.InsertMany(ctx, CreateDocs(numDocs*i+1, numDocs))
		require.NoError(t, err)
		require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
		err = syncCollection(ctx, ms.Sender, ms.Receiver, "test", 100, putOp)
		require.NoError(t, err)
		// check all records inserted
		c, err := receiverColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		require.Equal(t, numDocs*(i+1), c)
	}

	cancel()
	ms.WaitDone.Wait()
}
