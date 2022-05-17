package mongosync

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// getChangeStream tries to resume sync from last successfully committed syncId.
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

// getOplog creates channel providing oplog ops. It triggers(
//true) idle channel when there is not pending ops in the oplog and it isRToplog
func (ms *MongoSync) getOplog(ctx context.Context, db *mongo.Database, syncId string, syncTime time.Time, trace string) (Oplog, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	log.Infof("%s oplog started from %v, exchange %s", trace, syncTime, ms.Name())
	ch := make(chan bson.Raw, 1024) // need buffered to have a time before starting
	ms.routines.Add(1)              // getOplog
	// provide oplogST entries to the channel, closes channel upon exit
	go func() {
		defer func() {
			ms.routines.Done() // getOplog
			close(ch)
		}()
		for {
			next := changeStream.TryNext(ctx)
			if !next && ctx.Err() == nil {
				log.Tracef("getOplog: no pending oplog. pending buf %v pending bulkWrite %d, exchange: %s", ms.getCollUpdated(),
					ms.getPendingBulkWrite(), ms.Name())
				next = changeStream.Next(ctx)
				log.Infof("%s oplog idle finished: %s", trace, getOpName(changeStream.Current))
			}
			if next {
				ch <- changeStream.Current
				continue
			}
			err = ctx.Err()
			if errors.Is(err, context.Canceled) {
				log.Infof("%s oplog gracefully shutdown", trace)
				return
			} else if err != nil {
				log.Errorf("shutdown %s oplog : can't get next oplogST entry %s", trace, err)
				return
			}
			err := changeStream.Err()
			if err != nil {
				log.Errorf("failed to get %s oplog entry: %s", trace, err)
				continue
			}
		}
	}()
	return ch, nil
}
