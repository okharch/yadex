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
	"yadex/utils"
)

var cfg = &config.Config{
	Exchanges: []*config.ExchangeConfig{
		{
			SenderURI:   "mongodb://localhost:27021",
			SenderDB:    "test",
			ReceiverURI: "mongodb://localhost:27023",
			ReceiverDB:  "test",
			ST: map[string]*config.DataSync{".*": {
				Delay:   99,
				Batch:   1024 * 64, // bytes
				Exclude: []string{"realtime"},
			},
			},
		},
	},
}

func TestNewMongoSync(t *testing.T) {
	// create mongoSync
	config.SetLogger(log.InfoLevel, "")
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, cfg.Exchanges[0], ready)
	require.Nil(t, err)
	require.NotNil(t, ms)
	// wait for possible oplogST processing
}

// TestSync test
// 1. insert N records
// 2. sync to the receiver
// 3. insert another N records
// 4. sync to the receiver. This time it should be N records copied, not N*2
func TestSync(t *testing.T) {
	const countMany = int64(5000)
	const countLoop = int64(10)
	var ids []interface{}
	config.SetLogger(log.TraceLevel, "")
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, ExchCfg, ready)
	// drop sync bookmarks, test senderColl @ sender & receiver
	require.NoError(t, err)
	require.NotNil(t, ms)
	log.Infof("dropping MSync bookmarks")
	require.NoError(t, ms.syncId.Drop(ctx))
	const collName = "test"
	receiverColl := ms.Receiver.Collection(collName)
	log.Infof("Dropping %s collection @ receiver and sender ", collName)
	require.NoError(t, receiverColl.Drop(ctx))
	senderColl := ms.Sender.Collection(collName)
	require.NoError(t, senderColl.Drop(ctx))
	var wg sync.WaitGroup
	log.Info("Staring Sync.Run")
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Infof("Starting syncing %s", ms.Name())
		ms.Run(ctx)
		log.Info("Gracefully leaving mongoSync.Run")
	}()
	// start syncing
	log.Info("waiting sync to be ready")
	WaitState(ready, true, "sync to be ready")
	log.Info("starting syncing")
	started := time.Now()
	var filter interface{}
	log.Infof("Waiting InsertOne to arrive at the receiver %s", ms.Name())
	ir, err := senderColl.InsertOne(ctx, bson.D{{"name", "one"}})
	require.NoError(t, err)
	require.NoError(t, ms.WaitJobDone(time.Millisecond*500))
	filter = bson.M{"_id": ir.InsertedID}
	cc, err := receiverColl.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), cc)
	log.Infof("Waiting DeleteOne to arrive at the receiver %s", ms.Name())
	dr, err := senderColl.DeleteOne(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), dr.DeletedCount)
	require.NoError(t, ms.WaitJobDone(time.Millisecond*500))
	// remove 1 r before wait
	// delete inserted first
	c, err := receiverColl.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(0), c)
	log.Infof("Waiting InsertMany(%d) to arrive at the receiver %s", countMany*countLoop, ms.Name())
	for i := int64(0); i < countLoop; i++ {
		ir, err := senderColl.InsertMany(ctx, CreateDocs(i*countMany+1, countMany))
		require.NoError(t, err)
		ids = append(ids, ir.InsertedIDs...)
		//time.Sleep(time.Millisecond * 50)
	}
	require.NoError(t, ms.WaitJobDone(time.Second*5))
	c, err = receiverColl.CountDocuments(ctx, bson.M{"_id": bson.M{"$in": ids}})
	require.NoError(t, err)
	require.Equal(t, countMany*countLoop, c)
	duration := time.Since(started)
	log.Infof("Transferred %d bytes in %v, avg speed %s b/s", ms.totalBulkWrite, duration,
		utils.IntCommaB(ms.totalBulkWrite*int(time.Second)/int(duration)))
	cancel()
	wg.Wait()
}

func TestCollSync2(t *testing.T) {
	// 1. create some TestXX tables
	// 2. run sync
	// 3. check whether they were copied
	// 4. make some changes to TestXX and create TestNewXX
	// 5. Run Sync.
	// 6. Make sure everything synced
	// ----------------
	// make msync object and use tab
	config.SetLogger(log.TraceLevel, "")
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, ExchCfg, ready)
	// drop sync bookmarks, can be evil over time
	require.NoError(t, err)
	require.NotNil(t, ms)
	require.NoError(t, ms.syncId.Drop(ctx))
	collName := "test"
	count := 100
	coll := ms.Sender.Collection(collName)
	rcoll := ms.Receiver.Collection(collName)
	require.NoError(t, coll.Drop(ctx))
	require.NoError(t, rcoll.Drop(ctx))
	// 1. create some testXX
	ir, err := coll.InsertMany(ctx, CreateDocs(1, 100))
	require.NoError(t, err)
	require.Equal(t, count, len(ir.InsertedIDs))

	// 2. run sync
	go ms.Run(ctx)
	require.NoError(t, ms.WaitJobDone(time.Millisecond*100))
	// 3. check whether they were copied

	receiverIds := fetchIds(ctx, rcoll)
	require.Equal(t, count, len(receiverIds))
	filter := bson.M{"_id": bson.M{"$in": receiverIds}}
	lcount, err := coll.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(count), lcount)
	// 4. make some changes to TestXX and create TestNewXX
	// 5. Run Sync.
	// 6. Make sure everything synced

}
