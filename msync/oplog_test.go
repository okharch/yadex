package mongosync

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func TestGetDbOpLog(t *testing.T) {
	ctx := context.TODO()
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	require.NotNil(t, ms)
	ms.initSync(ctx)
	ctx, cancel := context.WithCancel(context.TODO())
	opCh, err := ms.GetDbOpLog(ctx, ms.Sender, "", ms.idleST)
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
}
