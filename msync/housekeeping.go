package mongosync

import (
	log "github.com/sirupsen/logrus"
	"time"
)

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

func (ms *MongoSync) getPendingBulkWrite() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	return ms.pendingBulkWrite
}

func (ms *MongoSync) getBWSpeed() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	totalDuration := time.Duration(0)
	totalBytes := 0
	for _, bwLog := range ms.bwLog {
		totalDuration += bwLog.duration
		totalBytes += bwLog.bytes
	}
	return totalBytes * int(time.Second) / int(totalDuration)
}

func (ms *MongoSync) addBulkWrite(delta int) {
	ms.bulkWriteMutex.Lock()
	ms.pendingBulkWrite += delta
	if delta > 0 {
		ms.totalBulkWrite += delta
	}
	ms.bulkWriteMutex.Unlock()
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
	//time.Sleep(timeout)
	select {
	case <-time.After(timeout):
		log.Tracef("WaitIdle:leaving on timer")
	case <-ms.idle:
		log.Tracef("WaitIdle:leaving on idle")
		return
	}
	<-ms.idle
}

func (ms *MongoSync) checkIdle(reason string) {
	pendingBulkWrite := ms.getPendingBulkWrite()
	if pendingBulkWrite == 0 {
		pendingBuffers := ms.getPendingBuffers()
		if pendingBuffers == 0 {
			handled := Signal(ms.idle)
			log.Tracef("checkIdle:%s:Sent IDLE signal, handled: %v", reason, handled)
		} else {
			ClearSignal(ms.idle)
			handled := Signal(ms.flush)
			log.Tracef("checkIdle:%s:Sent FLUSH signal (%d pending), handled: %v", reason, pendingBuffers, handled)
		}
	} else {
		ClearSignal(ms.idle)
		log.Tracef("checkIdle:%s: pending pending bw %d: no signals", reason, pendingBulkWrite)
	}
}

// Signal sends signal to channel non-blocking way
func Signal(ch chan struct{}) bool {
	// nb (non-blocking) empty buffered channel
	select {
	case ch <- struct{}{}:
		return true
	default:
	}
	return false
}

func ClearSignal(ch chan struct{}) {
	for {
		// nb read from channel until empty
		select {
		case <-ch:
		default:
			return
		}
	}
}
