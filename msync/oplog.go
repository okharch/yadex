package mongosync // getOpLog gets oplog and decodes it to bson.M. If it fails it returns nil
import (
	"context"
	"encoding/hex"
	"errors"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"time"
)

func getOpLog(ctx context.Context, changeStream *mongo.ChangeStream) (bson.M, error) {
	var op bson.M
	if changeStream.Next(ctx) && changeStream.Decode(&op) == nil {
		return op, nil
	}
	return nil, changeStream.Err()
}

// getChangeStream tries to resume sync from last successfully committed syncId.
// That id is stored for each mongo collection each time after successful write oplog into receiver database.
func getChangeStream(ctx context.Context, sender *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	opts.SetMaxAwaitTime(time.Millisecond * 50)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplog before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}

func GetDbOpLog(ctx context.Context, db *mongo.Database, syncId string) (<-chan bson.Raw, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw)
	go func() {
		defer close(ch)
		for {
			ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond*500)
			success := changeStream.Next(ctxTimeout)
			errCtx := ctxTimeout.Err()
			err := changeStream.Err()
			cancel()
			if errors.Is(errCtx, context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
				ch <- nil
				continue
			}
			if success {
				ch <- changeStream.Current
			}
			//err = changeStream.Err()
			if err == nil {
				continue
			}
			//if errors.Is(err, context.DeadlineExceeded) {
			//	// trigger idle operation on timeout
			//	ch <- nil
			//	continue
			//}
			if errors.Is(err, context.Canceled) {
				log.Info("Process oplog gracefully shutdown")
			} else {
				log.Errorf("shutdown oplog : can't get next oplog entry %s", err)
			}
			return
		}
	}()
	return ch, nil
}

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

// getLastSyncId retrieves where oplog is now.
// It inserts some record into random collection which it then drops
// it returns syncIid for insert operation.
func getLastSyncId(ctx context.Context, sender *mongo.Database) (string, error) {
	coll := sender.Collection("rnd" + GetRandomHex(8))
	changeStreamLastSync, err := getChangeStream(ctx, sender, "")
	if err != nil {
		return "", err
	}
	defer func() {
		// drop the temporary collection
		_ = changeStreamLastSync.Close(ctx)
		_ = coll.Drop(ctx)
	}()
	// make some update to generate oplog
	if _, err := coll.InsertOne(ctx, bson.M{"count": 1}); err != nil {
		return "", err
	}
	op, err := getOpLog(ctx, changeStreamLastSync)
	if op == nil {
		return "", err
	}
	return op["_id"].(bson.M)["_data"].(string), nil
}
