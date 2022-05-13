package mongosync

import (
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

func (ms *MongoSync) getPendingBulkWrite() int {
	ms.RLock()
	defer ms.RUnlock()
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
	// avoid divide by zero
	if totalDuration == 0 {
		return 0
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
	ms.RLock()
	defer ms.RUnlock()
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

func (ms *MongoSync) checkIdle(reason string, timeout time.Duration) {
	if timeout != 0 {
		time.Sleep(timeout) // give chance to flush buffers
	}
	ms.Lock()
	if ms.pendingBulkWrite == 0 && ms.pendingBuffers == 0 {
		// close channels, clear collChan
		log.Tracef("checkIdle: closing all colls channels...")
		for _, ch := range ms.collChan {
			close(ch)
		}
		// reset collChan map, no need to lock, it is
		ms.collChan = make(map[string]chan<- bson.Raw)
		ms.Unlock()
		handled := Signal(ms.idle)
		log.Infof("checkIdle:%s:Sent IDLE signal, handled: %v", reason, handled)
		return
	}
	ms.Unlock()
	if ms.getPendingBulkWrite() == 0 {
		pendingBuffers := ms.getPendingBuffers()
		ClearSignal(ms.idle)
		handled := Signal(ms.flush)
		log.Tracef("checkIdle:%s:Sent FLUSH signal (%d pending), handled: %v", reason, pendingBuffers, handled)
	} else {
		ClearSignal(ms.idle)
		log.Tracef("checkIdle:%s: pendingBulkWrite %d: no signals", reason, ms.pendingBulkWrite)
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
