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

var c = &config.Config{
	Exchanges: []*config.ExchangeConfig{
		{
			SenderURI:   "mongodb://localhost:27021",
			SenderDB:    "test",
			ReceiverURI: "mongodb://localhost:27023",
			ReceiverDB:  "test",
			RT: map[string]*config.DataSync{"realtime": {
				Delay:   99,
				Batch:   8192, // bytes
				Exclude: nil,
			},
			},
			ST: map[string]*config.DataSync{".*": {
				Delay:   999,
				Batch:   1024 * 128, // bytes
				Exclude: []string{"realtime"},
			},
			},
		},
	},
}

func TestNewMongoSync(t *testing.T) {
	// create mongoSync
	config.SetLogger(log.InfoLevel)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ms, err := NewMongoSync(ctx, c.Exchanges[0])
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
	config.SetLogger(log.TraceLevel)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ms, err := NewMongoSync(ctx, ExchCfg)
	// drop sync bookmarks, can be evil over time
	require.NoError(t, err)
	require.NotNil(t, ms)
	require.NoError(t, err)
	err = ms.syncId.Drop(ctx)
	require.Nil(t, err)
	const collName = "test"
	// start syncing
	started := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ms.Run(ctx)
		log.Info("Gracefully leaving mongoSync.Run")
	}()
	receiverColl := ms.Receiver.Collection(collName)
	require.NoError(t, receiverColl.Drop(ctx))
	coll := ms.Sender.Collection(collName)
	require.NoError(t, coll.Drop(ctx))
	ir, err := coll.InsertOne(ctx, bson.D{{"name", "one"}})
	const countMany = int64(50000)
	const countLoop = int64(10)
	var ids []interface{}
	for i := int64(0); i < countLoop; i++ {
		ir, err := coll.InsertMany(ctx, CreateDocs(i*countMany+1, countMany))
		require.NoError(t, err)
		ids = append(ids, ir.InsertedIDs...)
		time.Sleep(time.Millisecond * 100)
	}
	require.NoError(t, err)
	// remove 1 r before wait
	ms.WaitIdle(time.Millisecond * 100)
	// delete inserted first
	filter := bson.M{"_id": ir.InsertedID}
	dr, err := coll.DeleteOne(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), dr.DeletedCount)
	c, err := receiverColl.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), c)
	//time.Sleep(time.Second/2)
	ms.WaitIdle(time.Millisecond * 100)
	c, err = receiverColl.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(0), c)
	c, err = receiverColl.CountDocuments(ctx, bson.M{"_id": bson.M{"$in": ids}})
	require.NoError(t, err)
	require.Equal(t, countMany*countLoop, c)
	duration := time.Since(started)
	log.Infof("Transferred %d bytes in %v, avg speed %s b/s", ms.totalBulkWrite, duration,
		utils.IntCommaB(ms.totalBulkWrite*int(time.Second)/int(duration)))
}
