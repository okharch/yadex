package mongosync

import (
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

// initIdle creates channels collUpdates and idleChan used to implement checkIdle facility
func (ms *MongoSync) initIdle() {
	// this channel is used for non-blocking communication info about pending buffers in collection's channels
	ms.collUpdateTotal = make(map[string]int)
	ms.collSize = make(map[string]int)
	ms.idleChan = make(chan bool, 1)
	ms.routines.Add(1)
	go ms.idle()
}

func (ms *MongoSync) setIdleState(idleState bool) {
	ms.idleMutex.Lock()
	ms.idleState = idleState
	log.Tracef("idleState : %v", ms.idleState)
	ms.idleMutex.Unlock()
}

func (ms *MongoSync) getIdleState() bool {
	ms.idleMutex.RLock()
	defer ms.idleMutex.RUnlock()
	return ms.idleState
}

func (ms *MongoSync) getCollsBulkWriteTotal() int {
	ms.collBulkWriteMutex.RLock()
	defer ms.collBulkWriteMutex.RUnlock()
	return ms.collsBulkWriteTotal
}

func (ms *MongoSync) addCollsBulkWriteTotal(delta int) {
	ms.collBulkWriteMutex.Lock()
	defer ms.collBulkWriteMutex.Unlock()
	ms.collsBulkWriteTotal += delta
	log.Tracef("CollsBulkWriteTotal : %d", ms.collsBulkWriteTotal)
}

// idle follows idleChan and maintains idleState variable
// if it detects idle state and there is pending buffer for receiver it flushes that which is of max size
func (ms *MongoSync) idle() {
	defer ms.routines.Done()
	for idleState := range ms.idleChan {
		ms.setIdleState(idleState)
		for ms.getIdleState() && ms.getCollsUpdateTotal() > 0 {
			ms.flushUpdates()
		}
	}
}

// flushUpdates finds out collection with maximal size of BulkWrite and flush it to BulkWrite channel
func (ms *MongoSync) flushUpdates() {
	ms.collUpdateMutex.RLock()
	maxTotal := 0
	var maxColl string
	for coll, total := range ms.collUpdateTotal {
		if total > maxTotal {
			maxColl = coll
			maxTotal = total
		}
	}
	var collChan chan<- bson.Raw
	if maxTotal != 0 {
		log.Debugf("flushing coll %s %d bytes", maxColl, maxTotal)
		collChan = ms.collChan[maxColl]
		collChan <- nil // flush
	}
	ms.collUpdateMutex.RUnlock()
}

// updates size of  pending  coll's BulkWrite buffer
func (ms *MongoSync) setCollUpdateTotal(coll string, total int) {
	ms.collUpdateMutex.Lock()
	defer ms.collUpdateMutex.Unlock()
	oldTotal := ms.collUpdateTotal[coll]
	if total == 0 {
		delete(ms.collUpdateTotal, coll)
	} else {
		ms.collUpdateTotal[coll] = total
	}
	ms.collsUpdateTotal += total - oldTotal
	log.Tracef("collsUpdateTotal : %v", ms.collsUpdateTotal)
}

// WaitIdle detects situation when there is no pending input/output
func (ms *MongoSync) WaitIdle(timeout time.Duration) {
	for {
		time.Sleep(timeout)
		if ms.getIdleState() && ms.getCollsUpdateTotal() == 0 && ms.getCollsBulkWriteTotal() == 0 {
			return
		}
	}
}

// getCollsUpdateTotal returns total of bulkwrite buffers pending at coll's channels
func (ms *MongoSync) getCollsUpdateTotal() int {
	ms.collUpdateMutex.RLock()
	defer ms.collUpdateMutex.RUnlock()
	return ms.collsUpdateTotal
}
