package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
	"yadex/utils"
)

// BulkWriteOp describes postponed (bulkWrite) operation of modification which is sent to the receiver
type BulkWriteOp struct {
	Coll       string
	RealTime   bool
	OpType     OpLogType
	SyncId     string
	Models     []mongo.WriteModel
	TotalBytes int
	// RT related
	Lock    *sync.Mutex // avoid simultaneous BulkWrite for the same RT collection
	Expires time.Time   // time when RT batch expires
}

type BWLog struct {
	duration time.Duration
	bytes    int
}

func (ms *MongoSync) showSpeed(ctx context.Context) {
	defer ms.routines.Done()
	for range time.Tick(time.Second) {
		if ctx.Err() != nil {
			return
		}
		if ms.getPendingBuffers() > 0 {
			log.Tracef("BulkWriteOp: avg speed %s bytes/sec", utils.IntCommaB(ms.getBWSpeed()))
		}
	}
}

func (ms *MongoSync) closeOnCtxDone(ctx context.Context) {
	defer ms.routines.Done()
	<-ctx.Done()
	close(ms.oplogST)
	close(ms.oplogRT)
	close(ms.bulkWriteST)
	close(ms.bulkWriteRT)
	close(ms.idleST)
	close(ms.idleRT)
	close(ms.idle)
	close(ms.flush)
}

func (ms *MongoSync) runRTBulkWrite(ctx context.Context) {
	defer ms.routines.Done()
	const ConcurrentWrites = 3
	rtWrites := make(chan struct{}, ConcurrentWrites)
	log.Trace("runRTBulkWrite")
	for bwOp := range ms.bulkWriteRT {
		if ctx.Err() != nil {
			return
		}
		if bwOp.Expires.Before(time.Now()) {
			log.Warnf("drop batch for coll %s (%d/%d): expired: %v<%v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires, time.Now())
			ms.addBulkWrite(-bwOp.TotalBytes)
			continue
		}
		ms.wgRT.Add(1)
		go func(bwOp *BulkWriteOp) {
			bwOp.Lock.Lock()
			rtWrites <- struct{}{} // take write capacity, blocks if channel is full
			// as it might take time to acquire lock and write capacity, let's check expiration again before going write
			if bwOp.Expires.Before(time.Now()) {
				log.Warnf("drop batch for coll %s (%d/%d): expired: %v < %v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires,
					time.Now())
			} else if ctx.Err() == nil {
				ms.BulkWriteOp(ctx, bwOp)
			}
			ms.addBulkWrite(-bwOp.TotalBytes)
			<-rtWrites // return write capacity by popping value from channel
			bwOp.Lock.Unlock()
			ms.wgRT.Done()
		}(bwOp)
	}
}

// runSTBulkWrite serves bulkWriteST channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) runSTBulkWrite(ctx context.Context) {
	defer ms.routines.Done()
	log.Trace("runSTBulkWrite")
	for bwOp := range ms.bulkWriteST {
		log.Tracef("runSTBulkWrite:wgRT.Wait() %s %d", bwOp.Coll, bwOp.TotalBytes)
		ms.wgRT.Wait()
		ms.BulkWriteOp(ctx, bwOp)
		ms.addBulkWrite(-bwOp.TotalBytes)
	}
}

// putBwOp puts BulkWriteOp to either bulkWriteRT or bulkWriteST channel for BulkWrite operations.
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
	ms.addBulkWrite(bwOp.TotalBytes)
	if bwOp.RealTime {
		log.Tracef("putBwOp put RT %s %d(%d) records", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
		ms.bulkWriteRT <- bwOp
	} else {
		log.Tracef("putBwOp put ST %s %d(%d) records", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
		ms.bulkWriteST <- bwOp
	}
}

// BulkWriteOp BulkWrite bwOp.Models to sender, updates SyncId for the bwOp.Coll
func (ms *MongoSync) BulkWriteOp(ctx context.Context, bwOp *BulkWriteOp) {
	if ctx.Err() != nil {
		return
	}
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
	log.Tracef("BulkWriteOp %s %d records %d", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
	// add bwRecord, calculate speed
	start := time.Now()
	r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
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
	// update SyncId for the collection
	if !bwOp.RealTime && bwOp.SyncId != "" {
		upsert := true
		optUpsert := &options.ReplaceOptions{Upsert: &upsert}
		id := bson.E{Key: "_id", Value: bwOp.Coll}
		syncId := bson.E{Key: "sync_id", Value: bwOp.SyncId}
		updated := bson.E{Key: "updated", Value: primitive.NewDateTimeFromTime(time.Now())}
		filter := bson.D{id}
		doc := bson.D{id, syncId, updated}
		if r, err := ms.syncId.ReplaceOne(ctx, filter, doc, optUpsert); err != nil {
			log.Warnf("failed to update sync_id for collAtReceiver %s: %s", bwOp.Coll, err)
		} else {
			log.Tracef("Coll found(updated) %d(%d) %s.sync_id %s", r.MatchedCount, r.UpsertedCount+r.ModifiedCount, bwOp.Coll,
				bwOp.SyncId)
		}
	}
}
