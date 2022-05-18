package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
	"time"
	"yadex/config"
)

var ExchCfg = &config.ExchangeConfig{
	SenderURI: "mongodb://localhost:27021",
	SenderDB:  "test",
	ST: map[string]*config.DataSync{"test": {
		Delay:   200,
		Batch:   4096,
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
	config.SetLogger(log.InfoLevel)
	ctx, cancel := context.WithCancel(context.TODO())
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, ExchCfg, ready)
	require.NoError(t, err)
	require.NotNil(t, ms)
	// lets drop SyncId
	require.NoError(t, ms.syncId.Drop(ctx))
	const collName = "test"
	const numDocs = int64(113)
	collReceiver := ms.Receiver.Collection(collName)
	require.NoError(t, collReceiver.Drop(ctx))
	collSender := ms.Sender.Collection(collName)
	err = collSender.Drop(ctx)
	require.NoError(t, err)
	// create collSender at sender
	res, err := collSender.InsertMany(ctx, CreateDocs(1, numDocs))
	require.NoError(t, err)
	require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
	err = ms.initSync(ctx)
	require.NoError(t, err)
	// need runSTBulkWrite for SyncCollection
	ms.routines.Add(2)
	go ms.runDirt(ctx)
	go ms.runSTBulkWrite(ctx)
	// run syncCollection to transfer collSender from sender to receiver
	//err = ms.syncCollection(ctx, "test", 1024*128, "!")
	require.NoError(t, err)
	WaitState(ms.collsSyncDone, true, "colls sync done")
	SendState(ms.ready, true)
	require.NoError(t, ms.WaitJobDone(time.Millisecond*500))
	// now check what we have received at the receiver
	count, err := collReceiver.CountDocuments(ctx, bson.M{"_id": bson.M{"$in": res.InsertedIDs}})
	require.NoError(t, err)
	require.Equal(t, numDocs, count)
	// terminate ms sync
	cancel()
	//ms.routines.Wait()
}

// TestSyncCollection2Steps test
// 1. insert 100000 records
// 2. sync to the receiver
// 3. insert another 100000 records
// 4. sync to the receiver. This time it should be 100000 records copied, not 200000
func TestSyncCollectionMultiple(t *testing.T) {
	config.SetLogger(log.InfoLevel)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, ExchCfg, ready)
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

	for i := int64(0); i <= 3; i++ {
		// insert another 1000
		res, err := coll.InsertMany(ctx, CreateDocs(numDocs*i+1, numDocs))
		require.NoError(t, err)
		require.Equal(t, numDocs, int64(len(res.InsertedIDs)))
		err = ms.initSync(ctx)
		require.NoError(t, err)
		ms.routines.Add(1)
		go ms.runSTBulkWrite(ctx)
		// run syncCollection to transfer coll from sender to receiver
		//err = ms.syncCollection(ctx, "test", 1024*128, "!")
		require.NoError(t, err)
		//close(ms.oplogST)
		close(ms.bulkWriteST)
		ms.routines.Add(1)
		require.NoError(t, ms.WaitJobDone(time.Millisecond*500))
		// check all records inserted
		c, err := receiverColl.CountDocuments(ctx, bson.D{})
		require.NoError(t, err)
		require.Equal(t, numDocs*(i+1), c)
	}
	cancel()
	//ms.routines.Wait()
}
