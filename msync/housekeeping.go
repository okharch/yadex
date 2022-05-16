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

// updates size of  pending  coll's BulkWrite buffer
func (ms *MongoSync) setCollUpdated(coll string, updated bool) {
	ms.collBuffersMutex.Lock()
	if updated {
		ms.collUpdated[coll] = struct{}{}
	} else {
		delete(ms.collUpdated, coll)
	}
	ms.collBuffersMutex.Unlock()
}

// getPendingBuffers returns total of bulkwrite buffers pending at coll's channels
func (ms *MongoSync) getCollUpdated() bool {
	ms.collBuffersMutex.RLock()
	defer ms.collBuffersMutex.RUnlock()
	return len(ms.collUpdated) > 0
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

// runDirt serves dirty channel which triggered by various routines
// to check if msync is dirty.
// It finds out real state of the msync object and if it becomes changed
// it broadcasts that change using IsClean channel
// if it finds signal "non-dirty" (which it can receive from idling oplog or clean bulkwrite channel),
// it sends flush Signal (non-blocking) in order to flush pending buffers
func (ms *MongoSync) runDirt() {
	defer ms.routines.Done()
	oldDirty := true // consider it dirty at first so need to check real dirt after first clean signal
	for dirty := range ms.dirty {
		//log.Tracef("runDirt : %v old %v bw %d buf %d", dirty, oldDirty, ms.getPendingBulkWrite(), ms.getCollUpdated())
		if dirty == oldDirty {
			continue // ignore if state have not changed
		}
		if !dirty {
			// check if it is clean indeed
			if ms.getPendingBulkWrite() != 0 {
				continue // still dirty, don't change the state
			}
			// try to flush buffers, if they are not clean
			if ms.getCollUpdated() {
				Signal(ms.flush)
				continue // still dirty, don't change the state
			}
		}
		if dirty != oldDirty {
			log.Tracef("sending clean: %v", !dirty)
			SendState(ms.IsClean, !dirty)
			if !dirty {
				log.Infof("msync idling for changes...")
			} else {
				log.Infof("msync replicating changes...")
			}
		}
		oldDirty = dirty
	}
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
