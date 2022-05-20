package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
	"yadex/utils"
)

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

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

// WaitJobDone gets current ms.collUpdate state. If it is clean, it waits timeout if it stays clear all the time.
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

type BWLog struct {
	duration time.Duration
	bytes    int
}

func (ms *MongoSync) showSpeed(ctx context.Context) {
	defer ms.routines.Done() // showSpeed
	for range time.Tick(time.Second) {
		if ctx.Err() != nil {
			log.Debug("gracefully shutdown showSpeed on cancelled context")
			return
		}
		log.Tracef("BulkWriteOp: avg speed %s bytes/sec", utils.IntCommaB(ms.getBWSpeed()))
	}
}
