package mdb

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

func TestConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	uri := "mongodb://localhost:27021"
	clientOpts := options.Client().ApplyURI(uri).SetDirect(true)
	client, err := mongo.Connect(ctx, clientOpts)
	require.NoError(t, err, "test whether regular connection works")
	// try list databases
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	require.NoError(t, err, "test whether connection works ok")
	require.Greater(t, len(dbs), 0)
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
