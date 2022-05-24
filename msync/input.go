package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// getCollInput returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and
// flushes BulkWriteOp using appropriate BulkWrite channel
func (ms *MongoSync) getCollInput(ctx context.Context, collData *CollData) Oplog {
	if ctx.Err() != nil {
		return nil
	}
	collName := collData.CollName
	config := collData.Config
	OplogClass := collData.OplogClass
	// create buffered channel to handle ops for the collection
	// otherwise it will stall input for other colls
	input := make(Oplog, 1)
	id := 0
	// buffer for BulkWrite
	maxBatch := config.Batch
	flushDelay := time.Duration(config.Delay) * time.Millisecond
	log.Tracef("coll %s flush size %d", collName, maxBatch)
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	totalBytes := 0
	timer := time.AfterFunc(time.Hour, func() {})
	// flush bulkWriteST models or drop operation into BulkWrite channel
	flush := func(reason string) {
		timer.Stop()
		if ctx.Err() != nil {
			return
		}
		count := len(models)
		//log.Tracef("data for flush: %s %d %d", collName, totalBytes, len(models))
		if lastOpType != OpLogDrop {
			if count == 0 { // no op
				return // no flush
			}
		}
		// id, expires only for debugging
		id++
		bwOp := &BulkWriteOp{Coll: collName, OplogClass: OplogClass, OpType: lastOpType, CollData: collData, id: id, Expires: time.Now()}
		if bwOp.OplogClass == OplogRealtime {
			bwOp.Expires = time.Now().Add(time.Duration(config.Expires) * time.Millisecond)
		} else {
			bwOp.SyncId = getSyncId(lastOp)
		}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
			bwOp.TotalBytes = totalBytes
		}
		models = nil
		totalBytes = 0
		var ch chan *BulkWriteOp
		if OplogClass == OplogRealtime {
			ch = ms.bulkWriteRT
		} else {
			ch = ms.bulkWriteST
		}
		if ctx.Err() != nil {
			return
		}
		ch <- bwOp
		//_ = CancelSend(ctx, , bwOp, "bulkWriteRT")
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	ms.routines.Add(1) // collchan
	go func() {        // oplogST for collection
		defer ms.routines.Done() // colchan
		for op := range input {
			if op == nil {
				// this is from timer (go <-time.After(maxFlushDelay))
				// or from flushOnIdle
				flush("timeout/flushUpdate")
				continue
			}
			lenOp := len(op)
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flushOnIdle before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType && (lastOpType == OpLogOrdered || lastOpType == OpLogUnordered) {
					log.Tracef("coll %s flushOnIdle on opLogOrder %v", collName, opType)
					flush("changed OpLoOrder")
				}
			case OpLogDrop:
				models = nil
				lastOp = op
				lastOpType = opType
				// discard whatever buffer we have immediately
				models = nil
				totalBytes = lenOp
				// opLogDrop flushed immediately
				log.Tracef("coll %s flushOnIdle on opLogDrop", collName)
				flush("opLogDrop")
				continue
			case OpLogUnknown: // ignore op
				log.Warnf("Operation of unknown type left unhandled:%+v", op)
				continue
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			totalBytes += len(op)
			if totalBytes >= maxBatch {
				log.Tracef("coll %s flush on bulk size %d >= %d(%d)", collName, totalBytes, maxBatch, len(models))
				flush(fmt.Sprintf("%d > maxBatch %d", totalBytes+len(op), maxBatch))
			}

			if len(models) == 1 {
				// we just put 1st item, set timer to flushOnIdle after maxFlushDelay
				// unless it is filled up to (max)maxBatch items
				timer = time.AfterFunc(flushDelay, func() {
					collData.RLock()
					if collData.Input != nil {
						log.Tracef("flush on timer %v", flushDelay)
						CancelSend(ctx, collData.Input, nil) // flush on timer
					}
					collData.RUnlock()
				})
				log.Tracef("coll %s become collUpdate on op %s", collName, getOpName(op))
			}
		}
	}()

	return input
}
