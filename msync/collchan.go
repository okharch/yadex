package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
	"yadex/config"
)

// getCollChan returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and flushes BulkWriteOp using putBWOp func
func (ms *MongoSync) getCollChan(ctx context.Context, collName string, config *config.DataSync, realtime bool) Oplog {
	if ctx.Err() != nil {
		return make(chan bson.Raw, 1) // buffered, but ignore any input
	}
	var ftCount sync.WaitGroup
	ms.collChanMutex.RLock()
	sendCh, ok := ms.collChan[collName] // collChanMutex.RLock
	ms.collChanMutex.RUnlock()
	if ok {
		return sendCh
	}
	ch := make(chan bson.Raw)
	ms.collChanMutex.Lock()
	ms.collChan[collName] = ch // collChanMutex.Lock
	ms.collChanMutex.Unlock()
	var bwLock sync.Mutex
	// buffer for BulkWrite
	minFlushDelay, maxFlushDelay, maxBatch := config.MinDelay, config.Delay, config.Batch
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	totalBytes := 0
	// flush bulkWrite models or drop operation into BulkWrite channel
	flushed := time.Now()
	flush := func(reason string) {
		if ctx.Err() != nil {
			return
		}
		count := len(models)
		if lastOpType != OpLogDrop {
			if count == 0 { // no op
				return
			}
			if totalBytes < maxBatch && time.Since(flushed) < time.Millisecond*time.Duration(minFlushDelay) {
				return
			}
		}
		ftCancel()
		bwOp := &BulkWriteOp{Coll: collName, RealTime: realtime, OpType: lastOpType}
		if realtime {
			bwOp.Lock = &bwLock
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
		if realtime {
			ms.addBulkWrite(ctx, totalBytes)
			ms.setRTUpdated(collName, false)
		} else {
			ms.addBulkWrite(ctx, totalBytes)
			ms.setSTUpdated(collName, false)
		}
		totalBytes = 0
		ms.putBwOp(ctx, bwOp)
		flushed = time.Now()
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	go func() { // oplogST for collection
		defer ftCancel()
		for op := range ch {
			if op == nil {
				// this is from timer (go <-time.After(maxFlushDelay))
				// or from flush
				flush("timeout/flushUpdate")
				continue
			}
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flush before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType && (lastOpType == OpLogOrdered || lastOpType == OpLogUnordered) {
					log.Tracef("coll %s flush on opLogOrder %v", collName, opType)
					flush("changed OpLoOrder")
				}
			case OpLogDrop:
				models = nil
				lastOp = op
				lastOpType = opType
				totalBytes = 0
				// opLogDrop flushed immediately
				log.Tracef("coll %s flush on opLogDrop", collName)
				flush("opLogDrop")
				continue
			case OpLogUnknown: // ignore op
				log.Warnf("Operation of unknown type left unhandled:%+v", op)
				continue
			}
			if totalBytes+len(op) >= maxBatch {
				log.Tracef("coll %s flush on bulk size %d", collName, maxBatch)
				flush(fmt.Sprintf("%d > maxBatch %d", totalBytes+len(op), maxBatch))
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			totalBytes += len(op)

			if len(models) == 1 {
				// we just put 1st item, set timer to flush after maxFlushDelay
				// unless it is filled up to (max)maxBatch items
				ftCancel()
				ftCount.Wait()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				ftCount.Add(1)
				go func() {
					defer ftCount.Done()
					select {
					case <-flushTimer.Done():
						// we have done flush before maxFlushDelay, so no need to flush after timer expires
						return
					case <-time.After(time.Millisecond * time.Duration(maxFlushDelay)):
						ftCancel()
						log.Tracef("coll %s flush on timer %d ms", collName, maxFlushDelay)
						ch <- nil // flush() without lock
					}
				}()
				if realtime {
					ms.setRTUpdated(collName, true)
				} else {
					ms.setSTUpdated(collName, true)
				}
				log.Tracef("coll %s become dirty on op %s", collName, getOpName(op))
				if CancelSend(ctx, ms.dirty, true) { // update buffers become dirty, len(models) == 1
					return
				}
			}
		}
	}()

	return ch
}
