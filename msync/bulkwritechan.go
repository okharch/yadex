package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// initSync creates bwRT and bwST chan *BulkWriteOp  used to add bulkWrite operation to buffered channel
// also it initializes collection CollSyncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) initSync(ctx context.Context) {
	ms.collBuffers = make(map[string]int)
	// get name of bookmarkColSyncid on sender
	log.Tracef("initSync")
	// create buffered (3) BulkWrite channel
	ms.bwRT = make(chan *BulkWriteOp, 3)
	ms.bwST = make(chan *BulkWriteOp)
	// signalling channels
	ms.idle = make(chan struct{})
	ms.flushUpdates = make(chan struct{})

	ms.routines.Add(5)
	// close channels on expired context
	go func() {
		defer ms.routines.Done()
		<-ctx.Done()
		close(ms.bwST)
		close(ms.bwRT)
		close(ms.idle)
		close(ms.flushUpdates)
	}()

	// runSync few parallel serveBWChan() goroutines
	// 2 RT servers
	go ms.serveRTChan(ctx)
	go ms.serveRTChan(ctx)
	// 1 ST
	go ms.serveSTChan(ctx)
	// flushUpdates server
	go ms.serveFlushUpdates(ctx)
}

func (ms *MongoSync) serveRTChan(ctx context.Context) {
	defer ms.routines.Done()
	log.Trace("serveRTChan")
	for bwOp := range ms.bwRT {
		ms.wgRT.Add(1)
		ms.BulkWriteOp(ctx, bwOp)
		ms.wgRT.Done()
	}
}

// serveSTChan serves bwST channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) serveSTChan(ctx context.Context) {
	defer ms.routines.Done()
	log.Trace("serveSTChan")
	for bwOp := range ms.bwST {
		log.Tracef("serveSTChan:wgRT.Wait() %s %d", bwOp.Coll, bwOp.TotalBytes)
		ms.wgRT.Wait()
		ms.BulkWriteOp(ctx, bwOp)
	}
}

// putBwOp puts BulkWriteOp to either RT or ST channel for BulkWrite operations.
// Also, it puts signal to bwPut channel to inform serveBWChan
// there is a pending event in either of two channels.
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
	log.Tracef("putBwOp: %s %v %d", bwOp.Coll, bwOp.OpType, len(bwOp.Models))
	ms.addBulkWrite(bwOp.TotalBytes)
	if bwOp.RealTime {
		log.Tracef("putBwOp put RT %s %d(%d) records", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
		ms.bwRT <- bwOp
	} else {
		log.Tracef("putBwOp put ST %s %d(%d) records", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
		ms.bwST <- bwOp
	}
}

// BulkWriteOp BulkWrite bwOp.Models to sender, updates SyncId for the bwOp.Coll
func (ms *MongoSync) BulkWriteOp(ctx context.Context, bwOp *BulkWriteOp) {
	if ctx.Err() != nil {
		return
	}
	defer ms.addBulkWrite(-bwOp.TotalBytes)
	log.Tracef("BulkWriteOp %s %d", bwOp.Coll, bwOp.TotalBytes)
	collAtReceiver := ms.Receiver.Collection(bwOp.Coll)
	if bwOp.OpType == OpLogDrop {
		log.Tracef("drop Receiver.%s", bwOp.Coll)
		if err := collAtReceiver.Drop(ctx); err != nil {
			log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
		}
		return
	}
	ordered := bwOp.OpType == OpLogOrdered
	optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
	log.Tracef("BulkWriteOp %s %d records ", bwOp.Coll, len(bwOp.Models))
	r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
	if err != nil {
		// here we do not consider "DuplicateKey" as an error.
		// If the record with DuplicateKey is already on the receiver - it is fine
		if ordered || !mongo.IsDuplicateKeyError(err) {
			log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
			return
		}
	} else {
		log.Tracef("BulkWrite %s:%+v", bwOp.Coll, r)
	}
	// update SyncId for the collection
	if !bwOp.RealTime && bwOp.SyncId != "" {
		upsert := true
		optUpsert := &options.ReplaceOptions{Upsert: &upsert}
		id := bson.E{Key: "_id", Value: bwOp.Coll}
		syncId := bson.E{Key: "sync_id", Value: bwOp.SyncId}
		updated := bson.E{Key: "updated", Value: primitive.NewDateTimeFromTime(time.Now())}
		filter := bson.D{id}
		doc := bson.D{id, syncId, updated}
		if r, err := ms.CollSyncId.ReplaceOne(ctx, filter, doc, optUpsert); err != nil {
			log.Warnf("failed to update sync_id for collAtReceiver %s: %s", bwOp.Coll, err)
		} else {
			log.Tracef("Coll found(updated) %d(%d) %s.sync_id %s", r.MatchedCount, r.UpsertedCount+r.ModifiedCount, bwOp.Coll,
				bwOp.SyncId)
		}
	}
}
