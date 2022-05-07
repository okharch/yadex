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

// InitBulkWriteChan creates bwChan chan *BulkWriteOp field which is used to add bulkWrite operation to buffered channel
// this function puts BulkWrite operation only if this is RealTime op or channel is clean
// it launches goroutine which serves  operation from that channel and
// 1. sends bulkWrites to receiver mongodb
// 2. stores sync_id into CollSyncId for the ST collection on successful sync operation
func (ms *MongoSync) InitBulkWriteChan(ctx context.Context) {
	// get name of bookmarkColSyncid on sender
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.CollSyncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	// create buffered (3) BulkWrite channel
	ms.bwChan = make(chan *BulkWriteOp, 3)
	// ChanBusy is used to implement privileged access to BulkWriteOp channel for RT vs ST ops
	// it counts how many ops were put into the channel
	// ST ops always wait before  channel is empty and only then it is being put into channel
	// this goroutine serves BulkWriteOp channel
	serveBWChan := func() {
		for bwOp := range ms.bwChan {
			// check if context is not expired
			if ctx.Err() != nil {
				return
			}
			collAtReceiver := ms.Receiver.Collection(bwOp.Coll)
			if bwOp.OpType == OpLogDrop {
				log.Tracef("drop Receiver.%s", bwOp.Coll)
				if err := collAtReceiver.Drop(ctx); err != nil {
					log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
				}
			} else {
				ordered := bwOp.OpType == OpLogOrdered
				optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
				if len(bwOp.Models) > 0 {
					r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
					if err != nil {
						// here we do not consider "DuplicateKey" as an error.
						// If the record with DuplicateKey is already on the receiver - it is fine
						if ordered || !mongo.IsDuplicateKeyError(err) {
							log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
						}
					} else {
						log.Tracef("BulkWrite %s:%+v", bwOp.Coll, r)
					}
				}
			}
			// update sync_id for the ST collection
			if !bwOp.RealTime && bwOp.SyncId != "" {
				upsert := true
				optUpsert := &options.ReplaceOptions{Upsert: &upsert}
				id := bson.E{Key: "_id", Value: bwOp.Coll}
				syncId := bson.E{Key: "sync_id", Value: bwOp.SyncId}
				updated := bson.E{"updated", primitive.NewDateTimeFromTime(time.Now())}
				filter := bson.D{id}
				doc := bson.D{id, syncId, updated}
				if r, err := ms.CollSyncId.ReplaceOne(ctx, filter, doc, optUpsert); err != nil {
					log.Warnf("failed to update sync_id for collAtReceiver %s: %s", bwOp.Coll, err)
				} else {
					log.Tracef("Coll found(updated) %d(%d) %s.sync_id %s", r.MatchedCount, r.UpsertedCount+r.ModifiedCount, bwOp.Coll,
						bwOp.SyncId)
				}
			}
			ms.ChanBusy.Done()
		}
	}
	// runSync few parallel serveBWChan() goroutines
	for i := 0; i < 2; i++ {
		go serveBWChan()
	}

}

// putBwOp puts BulkWriteOp to BulkWriteChan.
// special value nil is used to make sure bwChan is empty
// It respects RT vs ST priority.
// It puts RT bwOp without delay
// It will wait until channel is empty and only then will put ST bwOp
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
	//log.Debugf("putBwOp: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
	if bwOp != nil && bwOp.RealTime {
		ms.ChanBusy.Add(1)
	} else {
		// this mutex needed to avoid two or more separate ST finding that channel is empty.
		// before waiting, it Locks and after ChanBusy.Add(1) it unlocks so
		// next ST will be waiting until channel is empty again
		ms.bwChanBusy.Lock() // one ST at a time
		//log.Debugf("putBwOp.Wait: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
		ms.ChanBusy.Wait() // wait until channel is clean
		if bwOp != nil {
			ms.ChanBusy.Add(1)
		}
		ms.bwChanBusy.Unlock()
	}
	//log.Debugf("putBwOp->channel: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
	if bwOp != nil {
		ms.bwChan <- bwOp
	}
	return
}
