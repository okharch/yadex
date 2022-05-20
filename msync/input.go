package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
)

// getCollInput returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and flushes BulkWriteOp using putBWOp func
func (ms *MongoSync) getCollInput(ctx context.Context, collData *CollData) {
	if ctx.Err() != nil {
		return
	}
	collName := collData.CollName
	config := collData.Config
	OplogClass := collData.OplogClass
	var ftCount sync.WaitGroup
	input := make(chan bson.Raw)
	collData.Input = input
	collData.Flushed = time.Now() // init CollData
	// buffer for BulkWrite
	maxFlushDelay, maxBatch := config.Delay, config.Batch
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	totalBytes := 0
	// flush bulkWrite models or drop operation into BulkWrite channel
	flush := func(reason string) {
		if ctx.Err() != nil {
			return
		}
		count := len(models)
		if lastOpType != OpLogDrop {
			if count == 0 { // no op
				return // no flush
			}
		}
		ftCancel()
		bwOp := &BulkWriteOp{Coll: collName, OplogClass: OplogClass, OpType: lastOpType, CollData: collData}
		if bwOp.OplogClass == olRT {
			bwOp.Expires = time.Now().Add(time.Duration(config.Expires) * time.Millisecond)
		} else {
			bwOp.SyncId = getSyncId(lastOp)
		}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
			bwOp.TotalBytes = totalBytes
		}
		models = nil
		log.Tracef("flusing %s %d %d due %s", collName, count, totalBytes, reason)
		totalBytes = 0
		_ = CancelSend(ctx, ms.bulkWrite, bwOp)
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	ms.routines.Add(1) // collchan
	go func() {        // oplogST for collection
		defer ms.routines.Done() // colchan
		defer ftCancel()
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
			if totalBytes+len(op) >= maxBatch {
				log.Tracef("coll %s flushOnIdle on bulk size %d", collName, maxBatch)
				flush(fmt.Sprintf("%d > maxBatch %d", totalBytes+len(op), maxBatch))
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			totalBytes += len(op)

			if len(models) == 1 {
				// we just put 1st item, set timer to flushOnIdle after maxFlushDelay
				// unless it is filled up to (max)maxBatch items
				ftCancel()
				ftCount.Wait()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				ftCount.Add(1)
				go func() {
					defer ftCount.Done()
					select {
					case <-flushTimer.Done():
						// we have done flushOnIdle before maxFlushDelay, so no need to flushOnIdle after timer expires
						return
					case <-time.After(time.Millisecond * time.Duration(maxFlushDelay)):
						ftCancel()
						log.Tracef("coll %s flush on timer %d ms", collName, maxFlushDelay)
						input <- nil // flush() without lock
					}
				}()
				log.Tracef("coll %s become collUpdate on op %s", collName, getOpName(op))
			}
		}
	}()

	return
}
