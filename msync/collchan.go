package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// getCollChan returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and flushes BulkWriteOp using putBWOp func
func (ms *MongoSync) getCollChan(collName string, maxDelay, maxBatch int, realtime bool) chan<- bson.Raw {
	in := make(chan bson.Raw)
	// buffer for BulkWrite
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	// flush bulkWrite models or drop operation into BulkWrite channel
	flush := func() {
		ftCancel()
		if len(models) == 0 && lastOpType != OpLogDrop { // no op
			return
		}
		var syncId string
		if !realtime {
			syncId = getSyncId(lastOp)
		}
		bwOp := &BulkWriteOp{Coll: collName, RealTime: realtime, OpType: lastOpType, SyncId: syncId}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
		}
		models = nil
		ms.collCountChan <- collCount{coll: collName, count: len(models)}

		ms.putBwOp(bwOp)
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	go func() { // oplog for collection
		defer ftCancel()
		for op := range in { // oplog
			if op == nil {
				// this is from timer (go <-time.After(maxDelay))
				// look below at case <-time.After(time.Millisecond * time.Duration(maxDelay)):
				flush()
				continue
			}
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flush before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType && (lastOpType == OpLogOrdered || lastOpType == OpLogUnordered) {
					flush()
				}
			case OpLogDrop:
				models = nil
				lastOp = op
				lastOpType = opType
				// opLogDrop flushed immediately
				flush()
				continue
			case OpLogUnknown: // ignore op
				log.Warnf("Operation of unknown type left unhandled:%+v", op)
				continue
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			if len(models) >= maxBatch {
				flush()
			} else if len(models) == 1 {
				// we just put 1st item, set timer to flush after maxDelay
				// unless it is filled up to (max)maxBatch items
				ftCancel()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				go func() {
					select {
					case <-flushTimer.Done():
						// we have done flush before maxDelay, so no need to flush after timer expires
						return
					case <-time.After(time.Millisecond * time.Duration(maxDelay)):
						ftCancel()
						in <- nil // flush() without lock
					}
				}()
			}
			ms.collCountChan <- collCount{coll: collName, count: len(models)}
		}
	}()
	return in
}
