package mdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	uri := "mongodb://localhost:27021"
	client, available, err := ConnectMongo(ctx, uri)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, available)
	avail := <-available
	require.True(t, avail)

	require.NoError(t, client.Disconnect(ctx))
	avail = <-available
	require.False(t, avail)

	client, available, err = ConnectMongo(ctx, uri)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, available)
	avail = <-available
	require.True(t, avail)
	// this works as well, but it requires manual intervention. If you want to check, uncomment and kill mongod
	//log.Infof("waiting for the server %s to be killed manually - what it will show?", uri)
	//avail = <- available
	//require.False(t, avail)
}
