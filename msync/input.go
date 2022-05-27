package mongosync

import (
	"context"
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
	lastFlushed := time.Now()
	flushDelay := time.Duration(config.Delay) * time.Millisecond
	log.Tracef("coll %s flush size %d", collName, maxBatch)
	var models []mongo.WriteModel
	lastOpType := OpLogUnknown
	var lastOp bson.Raw
	totalBytes := 0
	// flush bulkWriteST models or drop operation into BulkWrite channel
	flush := func(closeInput bool) {
		log.Infof("Input.flush %v", closeInput)
		if ctx.Err() != nil {
			return
		}
		// close input channel if required
		if closeInput {
			timerOn := <-collData.flushTimerOn // test whether timer is reset
			if timerOn && collData.Input != nil {
				close(collData.Input)
				collData.Input = nil
			}
			collData.flushTimerOn <- false // flush timer finished
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
		var bwChan chan *BulkWriteOp
		if OplogClass == OplogRealtime {
			bwChan = ms.bulkWriteRT
		} else {
			bwChan = ms.bulkWriteST
		}
		if ctx.Err() != nil {
			return
		}
		lastFlushed = time.Now() // after sending buffer to BulkWrite
		bwChan <- bwOp
		//_ = CancelSend(ctx, , bwOp, "bulkWriteRT")
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	ms.routines.Add(1) // collchan
	go func() {        // oplogST for collection
		defer ms.routines.Done() // colchan
		for op := range input {
			if op == nil {
				// this is from flushTimer (go <-time.After(maxFlushDelay))
				// or from flushOnIdle
				d := flushDelay - time.Since(lastFlushed)
				collData.flushTimer.Stop()
				<-collData.flushTimerOn
				collData.flushTimerOn <- false // reset previous timer
				<-collData.flushTimerOn
				collData.flushTimerOn <- true // set flush timer
				if d < 0 {
					flush(true)
				} else {
					collData.flushTimer.Reset(d)
				}
				continue
			}
			lenOp := len(op)
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flushOnIdle before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType && (lastOpType == OpLogOrdered || lastOpType == OpLogUnordered) {
					log.Tracef("coll %s flushOnIdle on opLogOrder %v", collName, opType)
					flush(false)
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
				flush(false)
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
				flush(false)
			}

			if len(models) == 1 {
				ms.ChangeColl <- ChangeColl{CollName: collName, SyncId: getSyncId(op)}
				// we just put 1st item
				// unless it is filled up to (max)maxBatch items
				log.Tracef("coll %s become collUpdate on op %s", collName, getOpName(op))
			}
		}
	}()

	return input
}
