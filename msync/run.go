package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
)

// Run launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used after Stop in a case of Exchange is not available (either sender or receiver)
// So it creates all channels and trying to resume from the safe point
func (ms *MongoSync) Run(ctx context.Context) {
	exCtx, exCancel := context.WithCancel(ctx)
	defer exCancel()
	senderAvail := false
	receiverAvail := false
	oldAvail := false
	var rsyncWG sync.WaitGroup
	// this loop watches over parent context and exchange's connections available
	// if they are not, it cancels exCtx so derived channels could be closed and synchronization stopped
	log.Tracef("ms.Run before available loop")
	for {
		select {
		case <-ctx.Done():
			ctx := context.TODO()
			_ = ms.senderClient.Disconnect(ctx)
			_ = ms.receiverClient.Disconnect(ctx)
			log.Tracef("ms.Run ExCancel...")
			exCancel()
			return
		case senderAvail = <-ms.SenderAvailable:
			if senderAvail {
				log.Infof("sender %s is available", ms.Config.SenderURI)
			} else {
				log.Warnf("sender %s is not available", ms.Config.SenderURI)
			}
		case receiverAvail = <-ms.ReceiverAvailable:
			if receiverAvail {
				log.Infof("receiver %s is available", ms.Config.ReceiverURI)
			} else {
				log.Warnf("receiver %s is not available", ms.Config.ReceiverURI)
			}
		}
		exAvail := senderAvail && receiverAvail
		if oldAvail != exAvail {
			sAvail, rAvail := "", ""
			if !senderAvail {
				sAvail = " not"
			}
			if !receiverAvail {
				rAvail = " not"
			}
			log.Debugf("sender:%s available, receiver:%s available", sAvail, rAvail)
			if exAvail {
				log.Infof("running exchange %s...", ms.Name())
				log.Infof("Connection available, start syncing of %s again...", ms.Name())
				// create func to put BulkWriteOp into channel. It deals appropriate both with Realtime and ST
				if err := ms.initSync(exCtx); err != nil {
					exCancel()
					log.Fatalf("init failed: %s", err)
				}
				SendState(ms.Ready, true, "ms.Ready")
				rsyncWG.Add(1)
				go func() {
					defer rsyncWG.Done()
					ms.runSync(exCtx)
				}()
			} else {
				SendState(ms.Ready, false, "ms.Ready")
				log.Warnf("Exchange unavailable, stopping %s... ", ms.Name())
				exCancel()
				rsyncWG.Wait()
				exCtx, exCancel = context.WithCancel(ctx)
			}
			oldAvail = exAvail
		}
	}
}

// runSync launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used in a case of Exchange is not available (either sender or receiver connection fails)
// it creates all the channels and trying to resume from the safe point
func (ms *MongoSync) runSync(ctx context.Context) {
	// you can count this using
	// git grep ms.routines.|grep -v _test|grep -v SyncCollections|grep -v  getOplog|grep -v runSToplog
	//ms.routines.Add(1)
	//go ms.runStatus(ctx)
	//go ms.flushOnTimer(ctx)
	//go ms.showSpeed(ctx) // optional, comment out if not needed
	// show avg speed of BulkWrite ops to the log
	log.Tracef("runSync running servers")
	if len(ms.Config.RT) > 0 {
		ms.routines.Add(2) // runBulkWriteRT, runRToplog
		go ms.runBulkWriteRT(ctx)
		go ms.runRToplog(ctx)
	}
	if len(ms.Config.ST) > 0 {
		ms.routines.Add(2) // runBulkWriteST, runSToplog
		go ms.runBulkWriteST(ctx)
		go ms.runSToplog(ctx)
	}
	log.Tracef("runSync:waiting for the cancel context")
	<-ctx.Done()
	// close all input channels
	log.Infof("waiting to shutdown sync exchange %s", ms.Name())
	ms.routines.Wait()
}

// runRToplog handles incoming oplogRT entries from oplogRT channel.
// It finds out which collection that oplogRT op belongs.
// If a channel for handling that collection has not been created,
// it calls getCollInput func to create that channel.
// Then it redirects oplogRT op to that channel.
func (ms *MongoSync) runRToplog(ctx context.Context) {
	defer ms.routines.Done() // runRToplog
	oplog, err := ms.getOplog(ctx, ms.Sender, "", OplogRealtime)
	log.Infof("Realtime oplog started for exchange %s", ms.Name())
	if err != nil {
		log.Fatalf("failed to init Realtime oplog: %s", err)
	}
	for op := range oplog {
		// we deal with the same db all the time,
		// it is enough to dispatch based on collName only
		collName := getOpColl(op)
		if collName == "" {
			continue
		}
	}
}

// initSTOplog finds out minimal sync_id from collMSync collection.
// which it can successfully resume oplogST watch.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplogST
// and returns empty collSyncId
// runSToplog handles incoming oplogST entries from oplogST channel.
// it calls getCollInput to redirect oplog to channel for an ST collection.
// If SyncId for the collection is greater than current syncId - it skips op
func (ms *MongoSync) runSToplog(ctx context.Context) {
	defer ms.routines.Done() // runSToplog
	log.Trace("runSToplog: SyncCollections")
	collSyncId, minSyncId, minTime, maxTime := ms.SyncCollections(ctx)
	if ctx.Err() != nil {
		return
	}
	// find out minimal start
	log.Trace("runSToplog: getOplog")
	oplog, err := ms.getOplog(ctx, ms.Sender, minSyncId, OplogStored)
	log.Infof("ST oplog started from %v, target => %v exchange %s", minTime.Time(), maxTime.Time(), ms.Name())
	if err != nil {
		log.Fatalf("failed to restore oplog for ST ops: %s", err)
	}
	// loop until context tells we are done
	bmExists := len(collSyncId) > 0
	lastColl := &CollData{}
	for op := range oplog {
		// we deal with the same db all the time,
		// it is enough to dispatch based on collName only
		collName := getOpColl(op)
		if collName == "" {
			continue
		}
		if bmExists {
			startAfter, ok := collSyncId[collName]
			if ok {
				syncId := getSyncId(op)
				if syncId <= startAfter.SyncId {
					//log.Tracef("skipping %s due %s <= %s", collName, syncId, startAfter.SyncId)
					continue
				}
				delete(collSyncId, collName)
				bmExists = len(collSyncId) > 0
			}
		}
		// check if it is subject to sync
		if lastColl.CollName != collName {
			curColl := ms.getCollData(collName)
			if curColl.Config == nil || curColl.OplogClass != OplogStored {
				continue
			}
			lastColl = curColl
		}
		ms.UpdateDirty(ctx, lastColl, len(op))
		lastColl.Input <- op
	}
}
