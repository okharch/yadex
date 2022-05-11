package mongosync

import (
	log "github.com/sirupsen/logrus"
	"time"
)

const idleTimeout = time.Millisecond * 50

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

func (ms *MongoSync) getPendingBulkWrite() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	return ms.pendingBulkWrite
}

func (ms *MongoSync) addBulkWrite(delta int) {
	ms.bulkWriteMutex.Lock()
	ms.pendingBulkWrite += delta
	ms.bulkWriteMutex.Unlock()
	log.Tracef("addBulkWrite bw:%d upd:%d idle:%v", ms.pendingBulkWrite, ms.getPendingBuffers(), ms.getOplogIdle())
	if ms.pendingBulkWrite == 0 && ms.getPendingBuffers() == 0 && ms.getOplogIdle() { // no need to check ms.getOplogIdle() ???
		Signal(ms.idle) // after flushed BulkWrite, most immediate Idle signal
	}
}

func (ms *MongoSync) getBulkWriteTotal() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	return ms.pendingBulkWrite
}

// updates size of  pending  coll's BulkWrite buffer
func (ms *MongoSync) setCollUpdateTotal(coll string, total int) {
	ms.collBuffersMutex.Lock()
	defer ms.collBuffersMutex.Unlock()
	oldTotal := ms.collBuffers[coll]
	if total == 0 {
		delete(ms.collBuffers, coll)
	} else {
		ms.collBuffers[coll] = total
	}
	ms.pendingBuffers += total - oldTotal
	log.Tracef("setCollUpdateTotal:pendingBuffers : %v", ms.pendingBuffers)
}

// getPendingBuffers returns total of bulkwrite buffers pending at coll's channels
func (ms *MongoSync) getPendingBuffers() int {
	ms.collBuffersMutex.RLock()
	defer ms.collBuffersMutex.RUnlock()
	return ms.pendingBuffers
}

// WaitIdle wait predefined period and then waits until all input/output/updated channels cleared
func (ms *MongoSync) WaitIdle(timeout time.Duration) {
	log.Infof("WaitIdle :timeout %v", timeout)
	for {
		select {
		case <-ms.idle:
			log.Tracef("WaitIdle:leaving on idle")
			return
		case <-time.After(timeout):
			log.Tracef("WaitIdle:checking idle %v buf %d bw %d", ms.getOplogIdle(), ms.getPendingBuffers(), ms.getPendingBulkWrite())
			if ms.getOplogIdle() && ms.getPendingBuffers() == 0 && ms.getPendingBulkWrite() == 0 {
				log.Tracef("WaitIdle:found done, leaving")
				return
			}
		}
	}
}

func (ms *MongoSync) getOplogIdle() bool {
	ms.oplogMutex.RLock()
	defer ms.oplogMutex.RUnlock()
	return ms.oplogIdle
}

func (ms *MongoSync) setOplogIdle(val bool) {
	if !val && !ms.getOplogIdle() {
		return
	}
	ms.oplogMutex.Lock()
	ms.oplogIdle = val
	ms.oplogMutex.Unlock()
	if val && ms.getPendingBulkWrite() == 0 {
		if ms.getPendingBuffers() == 0 {
			Signal(ms.idle)
			log.Tracef("setOplogIdle %v. Sent IDLE signal", val)
		} else {
			Signal(ms.flushUpdates)
			log.Tracef("setOplogIdle %v. Sent FLUSH signal", val)
		}
	} else {
		log.Tracef("setOplogIdle %v buf %d bw %d. No signals", val, ms.getPendingBuffers(), ms.getPendingBulkWrite())
	}

}

// Signal sends signal to channel non-blocking way
func Signal(ch chan struct{}) {
	// nb (non-blocking) empty buffered channel
	select {
	case ch <- struct{}{}:
	default:
	}
}
