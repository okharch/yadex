package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
	"yadex/config"
)

// getCollChan returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and flushes BulkWriteOp using putBWOp func
func (ms *MongoSync) getCollChan(ctx context.Context, collName string, config *config.DataSync, realtime bool) chan<- bson.Raw {
	in := make(chan bson.Raw, 2) // non-blocking, otherwise setCollUpdateTotal might deadlock
	// buffer for BulkWrite
	minFlushDelay, maxFlushDelay, maxBatch := config.MinDelay, config.Delay, config.Batch
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	totalBytes := 0
	// flush bulkWrite models or drop operation into BulkWrite channel
	flushed := time.Now()
	flush := func() {
		if ctx.Err() != nil {
			return
		}
		count := len(models)
		if lastOpType != OpLogDrop {
			if count == 0 { // no op
				return
			}
			if count < maxBatch && time.Since(flushed) < time.Millisecond*time.Duration(minFlushDelay) {
				return
			}
		}
		ftCancel()
		var syncId string
		if !realtime {
			syncId = getSyncId(lastOp)
		}
		bwOp := &BulkWriteOp{Coll: collName, RealTime: realtime, OpType: lastOpType, SyncId: syncId}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
			bwOp.TotalBytes = totalBytes
		}
		models = nil
		totalBytes = 0
		ms.setCollUpdateTotal(collName, totalBytes)
		ms.putBwOp(bwOp)
		flushed = time.Now()
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	go func() { // oplog for collection
		defer ftCancel()
		for { // oplog can be buffered, so w
			var op bson.Raw
			select {
			case <-ctx.Done():
				return
			case op = <-in:
			}
			if op == nil {
				// this is from timer (go <-time.After(maxFlushDelay))
				// or from flushUpdates
				flush()
				continue
			}
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flush before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType && (lastOpType == OpLogOrdered || lastOpType == OpLogUnordered) {
					log.Tracef("coll %s flush on opLogOrder %v", collName, opType)
					flush()
				}
			case OpLogDrop:
				models = nil
				lastOp = op
				lastOpType = opType
				// opLogDrop flushed immediately
				log.Tracef("coll %s flush on opLogDrop", collName)
				flush()
				continue
			case OpLogUnknown: // ignore op
				log.Warnf("Operation of unknown type left unhandled:%+v", op)
				continue
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			ms.setCollUpdateTotal(collName, totalBytes)
			totalBytes += len(op)
			if totalBytes >= maxBatch {
				log.Tracef("coll %s flush on bulk size %d", collName, maxBatch)
				flush()
			} else if len(models) == 1 {
				// we just put 1st item, set timer to flush after maxFlushDelay
				// unless it is filled up to (max)maxBatch items
				ftCancel()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				go func() {
					select {
					case <-flushTimer.Done():
						// we have done flush before maxFlushDelay, so no need to flush after timer expires
						return
					case <-time.After(time.Millisecond * time.Duration(maxFlushDelay)):
						ftCancel()
						log.Tracef("coll %s flush on timer %d ms", collName, maxFlushDelay)
						in <- nil // flush() without lock
					}
				}()
			}
		}
	}()

	return in
}

// flushUpdates sends "flush" signal to all the channels having dirty buffer
func (ms *MongoSync) serveFlushUpdates(ctx context.Context) {
	defer ms.routines.Done()
	for range ms.flushUpdates {
		ms.collBuffersMutex.RLock()
		collChans := make([]chan<- bson.Raw, len(ms.collBuffers))
		count := 0
		for coll := range ms.collBuffers {
			collChans[count] = ms.collChan[coll]
			count++
		}
		ms.collBuffersMutex.RUnlock()
		for _, collChan := range collChans {
			if ctx.Err() != nil {
				return
			}
			collChan <- nil // flush
		}
	}
}
