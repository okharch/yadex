package mongosync

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

// TestIdIncrement shows that minimal increment for primitive.DateTime,
// which is used for storing time in mongodb, is millisecond
func TestIdIncrement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	uri := "mongodb://localhost:27021"
	clientOpts := options.Client().ApplyURI(uri).SetDirect(true)
	client, err := mongo.Connect(ctx, clientOpts)
	require.NoError(t, err, "connection works")
	// create record
	coll := client.Database("test").Collection("test")
	require.NoError(t, coll.Drop(ctx))
	require.NotNil(t, coll)
	now := time.Now()
	id := primitive.NewDateTimeFromTime(now)
	ir, err := coll.InsertOne(ctx, bson.D{{"_id", now}})
	require.NoError(t, err)
	require.Equal(t, id, ir.InsertedID)
	diff := now.Sub(id.Time())
	// show that precision is millisecond (1e-3, while nanosecond is 1e-9)
	require.Equal(t, int(diff), now.Nanosecond()%1000000)
	// show that the precision of primitive.DateTime is Millisecond
	// add almost microsecond - no difference
	id1 := primitive.NewDateTimeFromTime(id.Time().Add(time.Nanosecond * 999))
	require.Equal(t, id, id1)
	// add almost millisecond - no difference, it is truncated
	id1 = primitive.NewDateTimeFromTime(id.Time().Add(time.Microsecond * 999))
	require.Equal(t, id, id1)
	// add millisecond - now it is different
	id1 = primitive.NewDateTimeFromTime(id.Time().Add(time.Millisecond))
	require.NotEqual(t, id, id1)
	ir, err = coll.InsertOne(ctx, bson.D{{"_id", id1}})
	require.NoError(t, err)
	require.Equal(t, id1, ir.InsertedID)
}
