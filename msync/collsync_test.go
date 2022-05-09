package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"time"
	"yadex/config"
)

var ExchCfg = &config.ExchangeConfig{
	SenderURI: "mongodb://localhost:27021",
	SenderDB:  "test",
	ST: map[string]*config.DataSync{"test": {
		Delay:   10,
		Batch:   97,
		Exclude: nil,
	},
	},
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

// TestSyncCollection simple test which
// 1. creates collection of 1000 docs at sender
// 2. copies it to the receiver using syncCollection routine
// 3. checks 1000 records delivered
func TestSyncCollection(t *testing.T) {
	config.SetLogger()
	ctx, cancel := context.WithCancel(context.TODO())
	var waitSync sync.WaitGroup
	ms, err := NewMongoSync(ctx, ExchCfg, &waitSync)
	require.NoError(t, err)
	require.NotNil(t, ms)
	const collName = "test"
	const numDocs = int64(113)
	coll := ms.Receiver.Collection(collName)
	require.NoError(t, coll.Drop(ctx))
	coll = ms.Sender.Collection(collName)
	err = coll.Drop(ctx)
	require.NoError(t, err)
	// create coll at sender
	res, err := coll.InsertMany(ctx, CreateDocs(1, numDocs))
	require.NoError(t, err)
	require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
	ms.InitBulkWriteChan(ctx)
	lastSyncId, err := getLastSyncId(ctx, ms.Sender)
	require.NoError(t, err)
	// run syncCollection to transfer coll from sender to receiver
	err = ms.syncCollection(ctx, "test", 97, lastSyncId)
	require.NoError(t, err)
	// there is no oplog, idle must be set manually
	ms.setIdleState(true)
	// wait until data transferred through BulkWrite channel
	ms.WaitIdle(time.Millisecond)
	// now check what we have received at the receiver
	count, err := ms.Receiver.Collection(collName).CountDocuments(ctx, bson.M{"_id": bson.M{"$in": res.InsertedIDs}})
	require.NoError(t, err)
	require.Equal(t, numDocs, count)
	// terminate ms sync
	cancel()
}

// TestSyncCollection2Steps test
// 1. insert 100000 records
// 2. sync to the receiver
// 3. insert another 100000 records
// 4. sync to the receiver. This time it should be 100000 records copied, not 200000
func TestSyncCollectionMultiple(t *testing.T) {
	config.SetLogger()
	ctx, cancel := context.WithCancel(context.TODO())
	var waitSync sync.WaitGroup
	ms, err := NewMongoSync(ctx, ExchCfg, &waitSync)
	require.NoError(t, err)
	require.NotNil(t, ms)
	require.NoError(t, err)
	const collName = "test"
	const numDocs = int64(101)
	coll := ms.Sender.Collection(collName)
	err = coll.Drop(ctx)
	require.NoError(t, err)
	//mIds := Ids2Map(res.InsertedIDs)
	receiverColl := ms.Receiver.Collection(collName)
	err = receiverColl.Drop(ctx)
	require.NoError(t, err)

	// there is no oplog, idle must be set manually
	ms.setIdleState(true)
	ms.InitBulkWriteChan(ctx)
	for i := int64(0); i <= 3; i++ {
		// insert another 1000
		res, err := coll.InsertMany(ctx, CreateDocs(numDocs*i+1, numDocs))
		require.NoError(t, err)
		require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
		lastSyncId, err := getLastSyncId(ctx, ms.Sender)
		require.NoError(t, err)
		err = ms.syncCollection(ctx, "test", 217, lastSyncId)
		require.NoError(t, err)
		// wait until data transferred through BulkWrite channel
		ms.WaitIdle(time.Millisecond * 50)
		// check all records inserted
		c, err := receiverColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		require.Equal(t, numDocs*(i+1), c)
	}

	cancel()
	log.Info("waitSync.Wait()")
	waitSync.Wait()
}
