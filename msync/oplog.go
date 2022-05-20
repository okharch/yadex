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
	olRT     = 0
	olST     = 1
	olSyncId = 2
)

var ocName = [3]string{"RT", "ST", "SyncId"}

// getChangeStream tries to resume sync from last successfully committed SyncId.
// That id is stored for each mongo collection each time after successful write oplogST into receiver database.
func getChangeStream(ctx context.Context, sender *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplogST before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}

// provides oplog from changeStream,
func (ms *MongoSync) getOplog(ctx context.Context, db *mongo.Database, syncId string, oplogClass OplogClass) (Oplog,
	error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw)
	ms.routines.Add(1) // getOplog
	// provide oplogST entries to the channel, closes channel upon exit
	go func() {
		defer func() {
			ms.routines.Done() // getOplog
			close(ch)
		}()
		for {
			next := changeStream.TryNext(ctx)
			if !next {
				log.Trace("getOplog: idle")
				onIdleCtx, onIdleCancel := context.WithCancel(ctx)
				ms.routines.Add(1) // flushOnIdle
				go func() {
					ms.flushOnIdle(onIdleCtx, oplogClass)
					ms.routines.Done() // flushOnIdle
				}()
				next = changeStream.Next(ctx)
				onIdleCancel()
			}
			if next {
				if CancelSend(ctx, ch, changeStream.Current) {
					return
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			err := changeStream.Err()
			if err != nil {
				log.Errorf("failed to get %s oplog entry: %s", ocName[oplogClass], err)
				return
			}
		}
	}()
	return ch, nil
}
