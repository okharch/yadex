package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"
)

// flushOnIdle continue flush all Realtime/ST collections until context cancelled or job done
func (ms *MongoSync) flushOnIdle(ctx context.Context, oplogClass OplogClass) {
	name := ocName[oplogClass]
	ms.collDataMutex.RLock()
	defer ms.collDataMutex.RUnlock()
	for {
		idleTimeout := 0
		for _, collData := range ms.collData { // flush collData buffers by sending collData.Input <- nil
			if ctx.Err() != nil {
				log.Tracef("flushOnIdle: %s shutdown on cancelled context", name)
				return // otherwise, we might as well send nil to closed channel
			}
			collData.Lock()
			if collData.OplogClass == oplogClass && collData.Dirty > 0 {
				log.Tracef("flushOnIdle: %s %s...", name, collData.CollName)
				// do not flush until minFlushDelay milliseconds has passed since last flushed
				minFlushDelay := collData.Config.MinDelay
				passed := int(time.Since(collData.Flushed) / time.Millisecond)
				if passed >= minFlushDelay {
					log.Tracef("flushing coll %s %d bytes", collData.CollName, collData.Dirty)
					collData.Input <- nil
				} else {
					timeout := minFlushDelay - passed
					if timeout > idleTimeout {
						idleTimeout = timeout
					}
				}
			}
			collData.Unlock()
		}
		if idleTimeout == 0 {
			break
		}
		// this wait is interrupted by timeout or onIdleCancel() func on oplog.Next
		select {
		case <-ctx.Done():
			log.Tracef("flushOnIdle: cancelled")
			return
		case <-time.After(time.Duration(idleTimeout+10) * time.Millisecond):
		}
	}
	log.Tracef("flushOnIdle: %s done!", name)
	return
}
