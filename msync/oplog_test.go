package mongosync

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
	"yadex/mdb"
)

func TestGetDbOpLog(t *testing.T) {
	client, available, err := mdb.ConnectMongo(context.TODO(), "mongodb://localhost:27021")
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, available)
	ctx := context.TODO()
	opCh, err := GetDbOpLog(ctx, client.Database("test"), "")
	require.NoError(t, err)
	op := <-opCh
	require.Nil(t, op, "it should return nil op on timeout")
	ctx, cancel := context.WithCancel(context.TODO())
	ctx1, cancel1 := context.WithTimeout(ctx, time.Second*100)
	defer cancel1()
	cancel()
	require.True(t, errors.Is(ctx1.Err(), context.Canceled))
}
