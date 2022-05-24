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
		idleTimeout := time.Duration(0)
		var pending []*CollData
		ms.pendingMutex.RLock()
		for _, collData := range ms.pending { // flush collData buffers by sending collData.Input <- nil
			pending = append(pending, collData)
		}
		ms.pendingMutex.RUnlock()
		for _, collData := range pending {
			// it gets closed when collUpdate finds out that CollData.Dirty == 0
			if ctx.Err() != nil {
				log.Tracef("flushOnIdle: %s shutdown on cancelled context", name)
				return // otherwise, we might as well send nil to closed channel
			}
			collData.RLock()
			if collData.OplogClass == oplogClass && collData.Dirty > 0 {
				log.Tracef("flushOnIdle: %s %s...", name, collData.CollName)
				// do not flush until minFlushDelay milliseconds has passed since last flushed
				passed := time.Now().Sub(collData.Updated)
				d := time.Duration(collData.Config.MinDelay) * time.Millisecond
				if passed > d {
					log.Tracef("flushing coll %s %d bytes", collData.CollName, collData.Dirty)
					CancelSend(ctx, collData.Input, nil) // flushOnIdle
				} else {
					timeout := d - passed
					if timeout > idleTimeout {
						idleTimeout = timeout
					}
				}
			}
			collData.RUnlock()
		}
		if idleTimeout == 0 {
			// it means we have not found any dirty collectiona
			break
		}
		// this wait is interrupted by timeout or onIdleCancel() func on oplog.Next
		select {
		case <-ctx.Done():
			log.Tracef("flushOnIdle: cancelled")
			return
		case <-time.After(idleTimeout + 10*time.Millisecond):
		}
	}
	log.Tracef("flushOnIdle: %s done!", name)
	return
}

//func (ms *MongoSync) flushOnTimer(ctx context.Context) {
//	defer ms.routines.Done()
//	return
//	for range time.Tick(time.Millisecond * 100) {
//		ms.collDataMutex.RLock()
//		for _, collData := range ms.collData { // flush collData buffers by sending collData.Input <- nil
//			// it gets closed when collUpdate finds out that CollData.Dirty == 0
//			if ctx.Err() != nil {
//				log.Trace("flushOnTimer: shutdown on cancelled context")
//				ms.collDataMutex.RUnlock()
//				return // otherwise, we might as well send nil to closed channel
//			}
//			collData.RLock()
//			if collData.Input != nil && time.Now().After(collData.FlushAfter) {
//				collData.Input <- nil
//			}
//			collData.RUnlock()
//		}
//		ms.collDataMutex.RUnlock()
//	}
//	log.Tracef("flushOnTimer: done!")
//}
