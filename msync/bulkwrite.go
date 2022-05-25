package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// BulkWriteOp describes postponed (bulkWriteST) operation of modification which is sent to the receiver
type BulkWriteOp struct {
	Coll       string
	OplogClass OplogClass
	OpType     OpLogType
	Models     []mongo.WriteModel
	TotalBytes int
	CollData   *CollData
	SyncId     string
	id         int
	// others wait
	Expires time.Time // time when Realtime batch expires
}

func (bwOp *BulkWriteOp) Name() string {
	syncId := bwOp.SyncId
	if syncId != "" {
		syncId = getDiffId(syncId)
	} else {
		syncId = "!EMPTY!"
	}
	return syncId + "@" + bwOp.Coll //fmt.Sprintf("%s(%d)-%d", bwOp.Coll, bwOp.id, (bwOp.Expires.Nanosecond()/100)%1000)
}

// runBulkWriteST serves bulkWriteST channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) runBulkWriteST(ctx context.Context) {
	defer ms.routines.Done() // runBulkWriteST
	log.Trace("runBulkWriteST")
	for bwOp := range ms.bulkWriteST {
		if ctx.Err() != nil {
			log.Tracef("Leaving ST BW %s(%d)", bwOp.Coll, bwOp.TotalBytes)
			return
		}
		log.Tracef("ST BW %s(%d) , waiting RT", bwOp.Coll, bwOp.TotalBytes)
		ms.bulkWriteRTCount.Wait() // runBulkWriteST wait for Realtime writes
		ms.BulkWriteOp(ctx, bwOp)
	}
	log.Debug("runBulkWriteST gracefully shutdown on closed channel")
}

// runBulkWriteST serves bulkWriteST channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) runBulkWriteRT(ctx context.Context) {
	defer ms.routines.Done() // runBulkWriteRT
	log.Trace("runBulkWriteRT:start")
	// channel to limit number of cowrite routines. it has buffer of config.CoWrite size
	for bwOp := range ms.bulkWriteRT {
		if ctx.Err() != nil {
			return
		}
		if bwOp.Expires.Before(time.Now()) {
			log.Warnf("drop batch for coll %s (%d/%d): expired: %v<%v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires, time.Now())
			continue
		}
		ms.bulkWriteRTCount.Add(1) // bulkWriteRT: before proceed with Write
		ms.BulkWriteOp(ctx, bwOp)
		ms.bulkWriteRTCount.Done() // bulkWriteRT
	}
	log.Tracef("runBulkWriteRT: done")
}

// BulkWriteOp BulkWrite bwOp.Models to sender, updates SyncId for the bwOp.Coll
func (ms *MongoSync) BulkWriteOp(ctx context.Context, bwOp *BulkWriteOp) {
	if ctx.Err() != nil {
		return
	}
	//log.Tracef("BulkWriteOp op %s %d", bwOp.Name(), bwOp.TotalBytes)

	collAtReceiver := ms.Receiver.Collection(bwOp.Coll)
	start := time.Now()

	var err error
	// TODO: handling error here is critical.
	//  We must not proceed if error happens.
	//  Probably it is better to cancel exchange at all if can't repeat without error
	if bwOp.OpType == OpLogDrop {
		log.Tracef("BulkWriteOp drop Receiver.%s", bwOp.Coll)
		if err = collAtReceiver.Drop(ctx); err != nil {
			log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
		}
	} else {
		ordered := bwOp.OpType == OpLogOrdered
		optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
		// add bwRecord, calculate speed
		log.Tracef("BulkWriteOp %s %d records %d", bwOp.Name(), len(bwOp.Models), bwOp.TotalBytes)
		var r *mongo.BulkWriteResult
		r, err = collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
		if err != nil {
			// here we do not consider "DuplicateKey" as an error.
			// If the record with DuplicateKey is already on the receiver - it is fine
			if ordered || !mongo.IsDuplicateKeyError(err) {
				log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
			}
		} else {
			log.Tracef("BulkWriteOp: BulkWrite %s: %+v", bwOp.Name(), r)
		}
	}
	ms.UpdateDirty(ctx, bwOp.CollData, -bwOp.TotalBytes, nil)
	// update SyncId for the ST data
	if bwOp.OplogClass == OplogStored && bwOp.SyncId != "" {
		ms.WriteCollBookmark(ctx, bwOp.CollData, bwOp.SyncId)
	}
	// update speed
	duration := time.Now().Sub(start)
	if duration > 0 {
		ms.bulkWriteMutex.Lock()
		ms.bwLog[ms.bwLogIndex] = BWLog{duration: duration, bytes: bwOp.TotalBytes}
		ms.bwLogIndex++
		if ms.bwLogIndex == bwLogSize {
			ms.bwLogIndex = 0
		}
		ms.bulkWriteMutex.Unlock()
	}
}
