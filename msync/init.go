package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
)

// initSync creates bulkWriteRT and bulkWrite chan *BulkWriteOp  used to add bulkWrite operation to buffered channel
// also it initializes collection syncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) initSync(ctx context.Context) error {
	// compile all regexp for Realtime
	ms.rtMatch = compileRegexps(ms.Config.RT)
	ms.stMatch = compileRegexps(ms.Config.ST)
	// data for matching collName => collData
	ms.collData = make(map[string]*CollData)

	ms.IsClean = make(chan bool, 1) // this is state channel, must have capacity 1
	// signalling channels
	ms.collsSyncDone = make(chan bool, 1)
	ms.collUpdate = make(chan CollUpdate, 256) // send false if there is a chance to be IsClean
	ms.routines.Add(1)                         // runCollUpdate
	go ms.runCollUpdate(ctx)                   // serve collUpdate channel
	// init channels before serving routines
	ms.bulkWrite = make(chan *BulkWriteOp, 256)
	ms.bulkWriteRT = make(chan *BulkWriteOp, 256)
	ms.routines.Add(2) // runRTBulkWrite, runBulkWrite
	go ms.runBulkWrite(ctx)
	// get name of bookmarkColSyncid on sender
	log.Tracef("initSync")
	return nil
}
