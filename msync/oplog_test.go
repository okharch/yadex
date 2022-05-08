package mongosync

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"time"
)

func TestGetDbOpLog(t *testing.T) {
	var wg sync.WaitGroup
	ms, err := NewMongoSync(context.TODO(), ExchCfg, &wg)
	require.NoError(t, err)
	require.NotNil(t, ms)
	ctx, cancel := context.WithCancel(context.TODO())
	opCh, err := ms.GetDbOpLog(ctx, ms.Sender, "")
	time.Sleep(time.Millisecond * 100)
	require.True(t, ms.getIdleState())
	ir, err := ms.Sender.Collection("test").InsertOne(ctx, bson.D{{}})
	require.NoError(t, err)
	op := <-opCh
	require.NotNil(t, op)
	opType := op.Lookup("operationType")
	require.NotNil(t, opType)
	require.Equal(t, "insert", opType.StringValue())
	id := op.Lookup("fullDocument", "_id")
	require.NotNil(t, id)
	oid := id.ObjectID()
	require.Equal(t, ir.InsertedID, oid)
	cancel()
	require.True(t, errors.Is(ctx.Err(), context.Canceled))
	wg.Wait()
}
