package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"strings"
	"time"
)

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

// InitBulkWriteChan creates bwRT and bwST chan *BulkWriteOp  used to add bulkWrite operation to buffered channel
// also it initializes collection CollSyncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) InitBulkWriteChan(ctx context.Context) {
	// get name of bookmarkColSyncid on sender
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.CollSyncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	// create buffered (3) BulkWrite channel
	ms.bwRT = make(chan *BulkWriteOp, 3)
	ms.bwST = make(chan *BulkWriteOp, 1)
	ms.bwPut = make(chan struct{}, 3)

	// runSync few parallel serveBWChan() goroutines
	for i := 0; i < 2; i++ {
		ms.routines.Add(1)
		go ms.serveBWChan(ctx)
	}
}

// serveBWChan watches for any incoming BulkWrite operation
// then it first checks bwRT channel, if it is empty
// it gets BW op from bwST channel
// it uses BulkWriteOp method to send that BWOp to the receiver
func (ms *MongoSync) serveBWChan(ctx context.Context) {
	defer ms.routines.Done()
	for {
		// blocking wait any BulkWrite put to either channel
		select {
		case <-ctx.Done():
			return
		case <-ms.bwPut:
		}
		// either RT or ST pending
		// non-blocking check RT
		select {
		case bwOp := <-ms.bwRT:
			ms.BulkWriteOp(ctx, bwOp)
			continue
		default:
		}
		// non-blocking check ST
		select {
		case bwOp := <-ms.bwST:
			ms.BulkWriteOp(ctx, bwOp)
		default:
		}
	}
}

// putBwOp puts BulkWriteOp to either RT or ST channel for BulkWrite operations.
// Also, it puts signal to bwPut channel to inform serveBWChan
// there is a pending event in either of two channels.
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
	//log.Debugf("putBwOp: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
	if bwOp.RealTime {
		ms.bwRT <- bwOp
	} else {
		ms.bwST <- bwOp
	}
	ms.addCollsBulkWriteTotal(bwOp.TotalBytes)
	ms.bwPut <- struct{}{}
}

// BulkWriteOp BulkWrite bwOp.Models to sender, updates SyncId for the bwOp.Coll
func (ms *MongoSync) BulkWriteOp(ctx context.Context, bwOp *BulkWriteOp) {
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
	r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
	ms.addCollsBulkWriteTotal(-bwOp.TotalBytes)
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
