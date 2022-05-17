package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
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
	defer ms.routines.Done() // showSpeed
	for range time.Tick(time.Second) {
		if ctx.Err() != nil {
			log.Debug("gracefully shutdown showSpeed on cancelled context")
			return
		}
		if ms.getCollUpdated() {
			log.Tracef("BulkWriteOp: avg speed %s bytes/sec", utils.IntCommaB(ms.getBWSpeed()))
		}
	}
}

func (ms *MongoSync) runRTBulkWrite(ctx context.Context) {
	defer ms.routines.Done() // runRTBulkWrite
	const ConcurrentWrites = 3
	rtWrites := make(chan struct{}, ConcurrentWrites)
	log.Trace("runRTBulkWrite")
	for bwOp := range ms.bulkWriteRT {
		if ctx.Err() != nil {
			log.Debug("runRTBulkWrite:gracefully shutdown on cancelled context")
			return
		}
		if bwOp.Expires.Before(time.Now()) {
			log.Warnf("drop batch for coll %s (%d/%d): expired: %v<%v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires, time.Now())
			ms.addSTBulkWrite(-bwOp.TotalBytes)
			continue
		}
		ms.countBulkWriteRT.Add(1) // before proceed with Write
		go func(bwOp *BulkWriteOp) {
			defer ms.countBulkWriteRT.Done()
			bwOp.Lock.Lock()
			rtWrites <- struct{}{} // take write capacity, blocks if channel is full
			// as it might take time to acquire lock and write capacity, let's check expiration again before going write
			if bwOp.Expires.Before(time.Now()) {
				log.Warnf("drop batch for coll %s (%d/%d): expired: %v < %v", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes, bwOp.Expires,
					time.Now())
			} else if ctx.Err() == nil {
				ms.BulkWriteOp(ctx, bwOp)
			}
			ms.addSTBulkWrite(-bwOp.TotalBytes)
			<-rtWrites // return write capacity by popping value from channel
			bwOp.Lock.Unlock()
		}(bwOp)
	}
}

// runSTBulkWrite serves bulkWriteST channel, waits outputClear state before call ms.BulkWriteOp(ctx, bwOp)
func (ms *MongoSync) runSTBulkWrite(ctx context.Context) {
	defer ms.routines.Done() // runSTBulkWrite
	log.Trace("runSTBulkWrite")
	lastCtxTime := time.Now()
	for bwOp := range ms.bulkWriteST {
		if ctx.Err() != nil {
			if time.Since(lastCtxTime) > time.Second {
				log.Debug("runSTBulkWrite gracefully shutdown on expired context")
				return
			}
			continue
		}
		lastCtxTime = time.Now()
		// give a chance for RT channel
		ms.countBulkWriteRT.Wait() // runSTBulkWrite wait for RT writes
		ms.BulkWriteOp(ctx, bwOp)
		ms.addSTBulkWrite(-bwOp.TotalBytes)
	}
	log.Debug("runSTBulkWrite gracefully shutdown on closed channel")
}

// putBwOp puts BulkWriteOp to either bulkWriteRT or bulkWriteST channel for BulkWrite operations.
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
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
	log.Tracef("BulkWriteOp %s %d records %d", bwOp.Coll, len(bwOp.Models), bwOp.TotalBytes)
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
	if !(bwOp.RealTime || bwOp.SyncId == "") {
		ms.WriteCollBookmark(ctx, bwOp.Coll, bwOp.SyncId)
	}
}

// addSTBulkWrite modifies pendingSTBulkWrite and totalBulkWrite
// if it finds out there is no pendingSTBulkWrite it sends State false to ms.dirty
func (ms *MongoSync) addSTBulkWrite(delta int) {
	ms.bulkWriteMutex.Lock()
	ms.pendingSTBulkWrite += delta
	if delta > 0 {
		ms.totalBulkWrite += delta
	}
	bwClean := ms.pendingSTBulkWrite == 0
	ms.bulkWriteMutex.Unlock()
	// here there is a chance we become clean, let runDirt find it out
	if bwClean {
		log.Trace("pendingSTBulkWrite=0, check dirt<-false")
		ms.dirty <- false // BulkWrite buffers are clean
	}
}

// addSTBulkWrite modifies pendingSTBulkWrite and totalBulkWrite
// if it finds out there is no pendingSTBulkWrite it sends State false to ms.dirty
func (ms *MongoSync) addRTBulkWrite(delta int) {
	ms.bulkWriteMutex.Lock()
	ms.pendingRTBulkWrite += delta
	if delta > 0 {
		ms.totalBulkWrite += delta
	}
	bwClean := ms.pendingRTBulkWrite+ms.pendingSTBulkWrite == 0
	ms.bulkWriteMutex.Unlock()
	// here there is a chance we become clean, let runDirt find it out
	if bwClean {
		log.Trace("pendingSTBulkWrite=0, check dirt<-false")
		ms.dirty <- false // BulkWrite buffers are clean
	}
}

func (ms *MongoSync) WriteCollBookmark(ctx context.Context, collName, SyncId string) {
	updated := time.Now()
	doc := collSyncP{Updated: updated, SyncId: SyncId, CollName: collName}
	// purge previous bookmarks
	if prev, exist := ms.collBookmark[collName]; exist {
		if _, err := ms.syncId.DeleteOne(ctx, bson.M{"_id": prev}); err != nil {
			log.Errorf("failed to clean sync_id for %s: %s", collName, err)
		} else {
			log.Tracef("purged previous bookmark for %s", collName)
		}
		delete(ms.collBookmark, collName)
	}
	ms.collBuffersMutex.RLock()
	doc.Pending = Keys(ms.stUpdated)
	ms.collBuffersMutex.RUnlock()
	if len(doc.Pending) == 0 {
		ms.collBookmark[collName] = updated
	}
	if _, err := ms.syncId.InsertOne(ctx, &doc); err != nil {
		log.Errorf("failed to update sync_id for %s: %s", collName, err)
		return
	}
}
