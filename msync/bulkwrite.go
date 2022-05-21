package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

// BulkWriteOp describes postponed (bulkWrite) operation of modification which is sent to the receiver
type BulkWriteOp struct {
	Coll       string
	OplogClass OplogClass
	OpType     OpLogType
	SyncId     string
	Models     []mongo.WriteModel
	TotalBytes int
	CollData   *CollData
	// others wait
	Expires time.Time // time when Realtime batch expires
}

// runBulkWrite serves bulkWrite channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) runBulkWrite(ctx context.Context) {
	defer ms.routines.Done() // runBulkWrite
	log.Trace("runBulkWrite")
	var RTCount sync.WaitGroup
	// channel to limit number of cowrite routines. it has buffer of config.CoWrite size
	coWrite := make(chan struct{}, ms.Config.CoWrite)
	for bwOp := range ms.bulkWrite {
		if ctx.Err() != nil {
			return
		}
		if bwOp.OplogClass == OplogRealtime {
			if bwOp.Expires.Before(time.Now()) {
				log.Warnf("drop batch for coll %s (%d/%d): expired: %v<%v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires, time.Now())
				continue
			}
			RTCount.Add(1) // before proceed with Write
		} else {
			RTCount.Wait() // runBulkWrite wait for Realtime writes
		}
		coWrite <- struct{}{} // it waits for it's slot
		ms.routines.Add(1)    // Bulkwrite begin
		go func(bwOp *BulkWriteOp) {
			ms.BulkWriteOp(ctx, bwOp)
			ms.routines.Done() // Bulkwrite done
		}(bwOp)
	}
	log.Debug("runBulkWrite gracefully shutdown on closed channel")
}

// BulkWriteOp BulkWrite bwOp.Models to sender, updates SyncId for the bwOp.Coll
func (ms *MongoSync) BulkWriteOp(ctx context.Context, bwOp *BulkWriteOp) {
	if ctx.Err() != nil {
		return
	}
	log.Tracef("BulkWriteOp %s %d records %d", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
	collAtReceiver := ms.Receiver.Collection(bwOp.Coll)
	ordered := bwOp.OpType == OpLogOrdered
	if ordered {
		bwOp.CollData.Writing.Wait()
	}
	bwOp.CollData.Writing.Add(1)
	defer bwOp.CollData.Writing.Done()
	if bwOp.OpType == OpLogDrop {
		log.Tracef("drop Receiver.%s", bwOp.Coll)
		if err := collAtReceiver.Drop(ctx); err != nil {
			log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
		}
	}
	optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
	// add bwRecord, calculate speed
	start := time.Now()
	r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
	bwOp.CollData.Lock()
	bwOp.CollData.Flushed = time.Now()
	bwOp.CollData.Unlock()
	ms.collUpdate <- CollUpdate{CollName: bwOp.Coll, Delta: -bwOp.TotalBytes, OplogClass: bwOp.OplogClass} // BulkWriteOp
	if err != nil {
		// here we do not consider "DuplicateKey" as an error.
		// If the record with DuplicateKey is already on the receiver - it is fine
		if ordered || !mongo.IsDuplicateKeyError(err) {
			log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
			return
		}
	} else {
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
		log.Tracef("BulkWrite %s:%+v", bwOp.Coll, r)
	}
	// update SyncId for the ST data
	if !(bwOp.OplogClass == OplogRealtime || bwOp.SyncId == "") {
		ms.WriteCollBookmark(ctx, bwOp.CollData, bwOp.SyncId, time.Now())
	}
}
