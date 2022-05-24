package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

type OplogClass int

const (
	OplogRealtime = 0
	OplogStored   = 1
	OplogSyncId   = 2
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
	ch := make(chan bson.Raw, 256)
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
				if ctx.Err() != nil {
					log.Tracef("leaving oplog %s due cancelled context ", ocName[oplogClass])
					return
				}
				//SendState(ms.OplogIdle[oplogClass], true)
				onIdleCtx, onIdleCancel := context.WithCancel(ctx)
				ms.routines.Add(1) // flushOnIdle
				var waitIdle sync.WaitGroup
				waitIdle.Add(1)
				go func() {
					ms.flushOnIdle(onIdleCtx, oplogClass)
					ms.routines.Done() // flushOnIdle
					waitIdle.Done()
				}()

				log.Infof("getOplog %s: idle", ocName[oplogClass])
				next = changeStream.Next(ctx)
				onIdleCancel()
				waitIdle.Wait()
				log.Infof("getOplog %s: run %s", ocName[oplogClass], getOpColl(changeStream.Current))
			}
			if next {
				ch <- changeStream.Current
			}
		}
	}()
	return ch, nil
}

func (ms *MongoSync) getSyncIdOplog(ctx context.Context) (syncId chan string, cancel context.CancelFunc) {
	syncId = make(chan string, 1)
	var eCtx context.Context
	eCtx, cancel = context.WithCancel(ctx)
	oplog, err := ms.getOplog(eCtx, ms.Sender, "", OplogSyncId)
	if err != nil {
		log.Fatalf("failed to get oplog, perhaps it is not activated: %s", err)
	}
	ms.routines.Add(1) // SyncId channel
	go func() {
		for op := range oplog {
			SendState(syncId, getSyncId(op))
		}
		ms.routines.Done() // SyncId channel
	}()
	return
}
