package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type OplogClass int

const (
	OplogRealtime = 0
	OplogStored   = 1
	OplogSyncId   = 2
)

var ocName = [3]string{"RT", "ST", "SyncId"}

// GetChangeStream tries to resume sync from last successfully committed SyncId.
// That id is stored for each mongo collection each time after successful write oplogST into receiver database.
func GetChangeStream(ctx context.Context, sender *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
		//ts := primitive.Timestamp{}
		//opts.SetStartAtOperationTime(&ts)
	}
	var pipeline mongo.Pipeline
	// start watching oplogST before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}

// provides oplog from changeStream,
func (ms *MongoSync) getOplog(ctx context.Context, db *mongo.Database, syncId string, oplogClass OplogClass) (Oplog,
	error) {
	changeStream, err := GetChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw, 256)
	ms.routines.Add(1) // getOplog
	// provide oplogST entries to the channel, closes channel upon exit
	go func() {
		defer func() {
			ms.routines.Done() // getOplog
			close(ch)
		}()
		previous := false
		for {
			next := changeStream.TryNext(ctx)
			if !next {
				if ctx.Err() != nil {
					log.Tracef("leaving oplog %s due cancelled context ", ocName[oplogClass])
					return
				}
				// send nil to the channel before idling/sleep,
				// it will flush the changes from the last collection
				if previous {
					ch <- nil
				}

				log.Infof("getOplog %s: idle", ocName[oplogClass])
				next = changeStream.Next(ctx)
				log.Infof("getOplog %s: run %s", ocName[oplogClass], getOpColl(changeStream.Current))
			}
			if next {
				ch <- changeStream.Current
				previous = true
				continue
			}
			err := changeStream.Err()
			if err != nil {
				// TODO: try to handle oplog lost tail error
				log.Errorf("failed to get %s oplog entry: %s", ocName[oplogClass], err)
				return
			}
		}
	}()
	return ch, nil
}
