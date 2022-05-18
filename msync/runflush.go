package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"strings"
)

// runFlush sends "flush" signal to all the collection's channels having dirty buffer
func (ms *MongoSync) runFlush(ctx context.Context) {
	defer ms.routines.Done() // runFlush
	for range ms.flush {
		ms.collBuffersMutex.RLock()
		colls := Keys(ms.rtUpdated)
		ms.collBuffersMutex.RUnlock()
		log.Tracef("flusing buffers from %s...", strings.Join(colls, ", "))
		for _, coll := range colls {
			if ctx.Err() != nil {
				log.Debug("runFlush gracefully shutdown on cancelled context")
				return // otherwise, we might as well send nil to closed channel
			}
			ms.collChanMutex.RLock()
			collChan, ok := ms.collChan[coll] // collChanMutex.RLock()
			ms.collChanMutex.RUnlock()
			if ok {
				collChan <- nil
			}
		}
	}
	log.Debug("runFlush gracefully shutdown on closed channel")
}
