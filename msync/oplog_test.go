package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
)

func TestGetOplog(t *testing.T) {
	ctx := context.TODO()
	ready := make(chan bool, 1)
	ms, err := NewMongoSync(ctx, ExchCfg, ready)
	require.NoError(t, err)
	require.NotNil(t, ms)
	ctx, cancel := context.WithCancel(context.TODO())
	log.Infof("getting oplog for %s", ms.Name())
	opCh, err := ms.getOplog(ctx, ms.Sender, "")
	var input sync.WaitGroup
	input.Add(1)
	var op bson.Raw
	go func() {
		log.Info("getting OpCh")
		op = <-opCh
		db, collName := getNS(op)
		log.Infof("got OpCh %s.%s %s", db, collName, getSyncId(op))
		input.Done()
	}()
	log.Info("insert into test table")
	ir, err := ms.Sender.Collection("test").InsertOne(ctx, bson.D{})
	require.NoError(t, err)
	input.Wait()
	require.NotNil(t, op)
	opType := op.Lookup("operationType")
	require.NotNil(t, opType)
	require.Equal(t, "insert", opType.StringValue())
	id := op.Lookup("fullDocument", "_id")
	require.NotNil(t, id)
	oid := id.ObjectID()
	require.Equal(t, ir.InsertedID, oid)
	cancel()
	//require.True(t, errors.Is(ctx.Err(), context.Canceled))
}
