package mongosync

import (
	"fmt"
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
	var totalDuration time.Duration
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

// updates size of  pending  coll's BulkWrite buffer
func (ms *MongoSync) setSTUpdated(coll string, updated bool) {
	ms.collBuffersMutex.Lock()
	if updated {
		ms.stUpdated[coll] = struct{}{}
	} else {
		delete(ms.stUpdated, coll)
	}
	ms.collBuffersMutex.Unlock()
}

// updates size of  pending  coll's BulkWrite buffer
func (ms *MongoSync) setRTUpdated(coll string, updated bool) {
	ms.collBuffersMutex.Lock()
	if updated {
		ms.rtUpdated[coll] = struct{}{}
	} else {
		delete(ms.rtUpdated, coll)
	}
	ms.collBuffersMutex.Unlock()
}

// getPendingBuffers returns total of bulkwrite buffers pending at coll's channels
func (ms *MongoSync) getCollUpdated() bool {
	ms.collBuffersMutex.RLock()
	defer ms.collBuffersMutex.RUnlock()
	return len(ms.rtUpdated)+len(ms.stUpdated) > 0
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

// WaitJobDone gets current ms.dirty state. If it is clean, it waits timeout if it stays clear all the time.
// WaitJobDone is intended for unit tests.
// it also checks ms.ready and returns error if it is not
// It also checks if IsClean closed then it returns with error
func (ms *MongoSync) WaitJobDone(timeout time.Duration) error {
	WaitState(ms.ready, true, "ms.ready")
	ok := true
	for ok {
		if !GetState(ms.ready) {
			return fmt.Errorf("ms is not ready")
		}
		// we are clean here
		log.Trace("JobDone: waiting clean")
		var clean bool
		clean, ok = <-ms.IsClean
		log.Tracef("JobDone: got clean %v state", clean)
		if !clean {
			continue
		}
		log.Tracef("JobDone: waiting it is clean for %v", timeout)
		select {
		case clean, ok = <-ms.IsClean:
			log.Tracef("JobDone: got clean %v state(must be false)", clean)
			continue
		case <-time.After(timeout):
			log.Trace("JobDone: ready!")
			return nil
		}
	}
	return fmt.Errorf("JobDone: IsClean channel closed")
}
