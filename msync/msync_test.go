package mongosync

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"yadex/config"
)

var c = &config.Config{
	Exchanges: []*config.ExchangeConfig{
		{
			SenderURI:   "mongodb://localhost:27021",
			SenderDB:    "test",
			ReceiverURI: "mongodb://localhost:27023",
			ReceiverDB:  "test",
			RT: map[string]*config.DataSync{"realtime": {
				Delay:   50,
				Batch:   100,
				Exclude: nil,
			},
			},
			ST: map[string]*config.DataSync{".*": {
				Delay:   1000,
				Batch:   500,
				Exclude: []string{"realtime"},
			},
			},
		},
	},
}

func TestNewMongoSync(t *testing.T) {
	// create mongosync
	config.SetLogger()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	var waitExchanges sync.WaitGroup
	ms, err := NewMongoSync(ctx, c.Exchanges[0], &waitExchanges)
	require.Nil(t, err)
	require.NotNil(t, ms)
	// wait for possible oplog processing
}

// TestSync test
// 1. insert 100000 records
// 2. sync to the receiver
// 3. insert another 100000 records
// 4. sync to the receiver. This time it should be 100000 records copied, not 200000
func TestSync(t *testing.T) {
	config.SetLogger()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	var waitSync sync.WaitGroup
	ms, err := NewMongoSync(ctx, ExchCfg, &waitSync)
	require.NoError(t, err)
	require.NotNil(t, ms)
	require.NoError(t, err)
	const collName = "test"
	// start syncing
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ms.Run(ctx)
	}()
	coll := ms.Sender.Collection(collName)
	err = coll.Drop(ctx)
	require.NoError(t, err)
	receiverColl := ms.Receiver.Collection(collName)
	ir, err := coll.InsertOne(ctx, bson.D{{"name", "one"}})
	const countMany = int64(100)
	const countLoop = int64(5)
	var ids []interface{}
	for i := int64(0); i < countLoop; i++ {
		ir, err := coll.InsertMany(ctx, CreateDocs(i*countMany+1, countMany))
		require.NoError(t, err)
		ids = append(ids, ir.InsertedIDs...)
	}
	require.NoError(t, err)
	//time.Sleep(time.Second/2)
	ms.WaitFlushed()
	//time.Sleep(time.Millisecond*200)
	filter := bson.M{"_id": ir.InsertedID}
	c, err := receiverColl.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), c)
	//time.Sleep(time.Second/2)
	ms.WaitFlushed()
	c, err = receiverColl.CountDocuments(ctx, bson.M{"_id": bson.M{"$in": ids}})
	require.NoError(t, err)
	require.Equal(t, countMany*countLoop, c)
}
