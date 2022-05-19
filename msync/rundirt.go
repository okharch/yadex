package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// runDirty serves dirty channel which triggered by various routines
// to check if msync is dirty.
// It finds out real state of the msync object and if it becomes changed
// it broadcasts that change using IsClean channel
// if it finds signal "non-dirty" (which it can receive from idling oplog or clean bulkwrite channel),
// it sends flush Signal (non-blocking) in order to flush pending buffers
func (ms *MongoSync) runDirty(ctx context.Context) {
	defer ms.routines.Done() // runDirty
	oldDirty := true         // consider it dirty at first so need to check real dirt after first clean signal
	ssCtx, cancel := context.WithCancel(ctx)
	var ssWait sync.WaitGroup
	defer cancel()
	// showStatus shows status postponed by 500ms.
	// If new status arrives, the previous message is abandoned
	idling := "msync idling for changes, running GC..."
	showStatus := func(status string) {
		cancel()
		//log.Tracef("waiting cancelling %s", oldStatus)
		ssWait.Wait()
		//log.Tracef("Proceed with message %s", status)
		ssCtx, cancel = context.WithCancel(ctx)
		signal := status == ""
		timeout := time.Millisecond * 100
		if signal {
			timeout = time.Millisecond * 50
		}
		ms.routines.Add(1) // runDir.ShowStatus
		ssWait.Add(1)
		go func() {
			defer ms.routines.Done() // runDir.ShowStatus
			defer ssWait.Done()
			select {
			case <-ssCtx.Done():
				return
			case <-time.After(timeout):
				if signal {
					Signal(ms.flush)
				} else {
					log.Infof(status)
					if status == idling {
						//runtime.GC()
					}
				}
			}
		}()
	}
	for dirty := range ms.dirty {
		if ctx.Err() != nil {
			return
		}
		if dirty == oldDirty {
			continue // ignore if state have not changed
		}
		log.Tracef("runDirty : %v old %v bw %d buf %v", dirty, oldDirty, ms.getPendingBulkWrite(), ms.getCollUpdated())
		if !dirty {
			// check if it is clean indeed
			if ms.getPendingBulkWrite() != 0 {
				continue // still dirty, don't change the state
			}
			// try to flush buffers, if they are not clean
			if ms.getCollUpdated() {
				showStatus("") // empty message means postpone Signal(ms.flush) for 50ms
				continue       // still dirty, don't change the state
			}
		}
		if dirty != oldDirty {
			log.Tracef("sending clean: %v", !dirty)
			SendState(ms.IsClean, !dirty)
			if dirty {
				showStatus("msync replicating changes...")
			} else {
				showStatus(idling)
			}
		}
		oldDirty = dirty
	}
}
