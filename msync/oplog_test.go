package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
	"testing"
	"time"
)

func TestGetOplog(t *testing.T) {
	ctx := context.TODO()
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	require.NotNil(t, ms)
	ctx, cancel := context.WithCancel(context.TODO())
	log.Infof("getting oplog for %s", ms.Name())
	opCh, err := ms.getOplog(ctx, ms.Sender, "", OplogStored)
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
	timeout := time.Duration(time.Second)
	log.Infof("sleep %v", timeout)
	time.Sleep(timeout)
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

func TestOplogDbs(t *testing.T) {
	ctx := context.TODO()
	ms, err := NewMongoSync(ctx, ExchCfg)
	require.NoError(t, err)
	require.NotNil(t, ms)
	ctx, cancel := context.WithCancel(context.TODO())
	log.Infof("getting oplog for %s", ms.Name())
	opCh, err := ms.getOplog(ctx, ms.Sender, "", OplogStored)
	getOpTimeout := func() (timeout bool, coll string) {
		select {
		case op := <-opCh:
			db, coll := getNS(op)
			return false, db + "." + coll
		case <-time.After(time.Second):
			return true, ""
		}
	}
	log.Info("insert into test table")
	_, err = ms.Sender.Collection("test").InsertOne(ctx, bson.D{})
	require.NoError(t, err)
	timeout, coll := getOpTimeout()
	require.False(t, timeout)
	expect := ms.Sender.Name() + "." + "test"
	require.Equal(t, expect, coll)
	log.Info("insert the same client but different database")
	db1 := "test1"
	_, err = ms.senderClient.Database(db1).Collection("test1").InsertOne(ctx, bson.D{})
	require.NoError(t, err)
	timeout, coll = getOpTimeout()
	require.True(t, timeout)
	require.Equal(t, "", coll)
	cancel()
	ms.routines.Wait()
	_, ok := <-opCh
	require.False(t, ok, "expect channel to be closed")
	//require.True(t, errors.Is(ctx.Err(), context.Canceled))
}
