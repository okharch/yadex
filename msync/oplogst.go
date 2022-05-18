package mongosync

import (
	"context"
	"encoding/hex"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"math/rand"
	"time"
)

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

type collSyncP struct {
	SyncId   string    `bson:"sync_id"`  // SyncId to restore syncing
	CollName string    `bson:"collName"` // Coll which had been flushed
	Pending  []string  `bson:"pending"`  // pending collections at the time of flushing
	Updated  time.Time `bson:"_id"`      // ST collection is written one at a time,
	// hopefully it will be protection against duplicate key
}

// initSTOplog finds out minimal sync_id from collMSync collection.
// which it can successfully resume oplogST watch.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplogST
// and returns empty collSyncId
func (ms *MongoSync) initSTOplog(ctx context.Context) {
	collSyncId, minSyncId, minTime, maxTime := ms.SyncCollections(ctx)
	if ctx.Err() != nil {
		return
	}
	// find out minimal start
	oplog, err := ms.getOplog(ctx, ms.Sender, minSyncId, "ST")
	log.Infof("ST oplog started from %v, target => %v exchange %s", minTime, maxTime, ms.Name())
	if err != nil {
		log.Fatalf("failed to restore oplog for ST ops: %s", err)
	}
	ms.routines.Add(1) // go ms.runSToplog
	go ms.runSToplog(ctx, oplog, collSyncId)
	return
}

// runSToplog handles incoming oplogST entries from oplogST channel.
// it calls getCollChan to redirect oplog to channel for an ST collection.
// If SyncId for the collection is greater than current syncId - it skips op
func (ms *MongoSync) runSToplog(ctx context.Context, oplog Oplog, collSyncId map[string]*CollBookmark) {
	defer ms.routines.Done() // runSToplog
	// loop until context tells we are done
	log.Trace("running SToplog")
	for {
		var op bson.Raw
		ok := true
		select {
		case op, ok = <-oplog:
			if !ok {
				return
			}
		case <-time.After(time.Millisecond):
			ok = false
			if CancelSend(ctx, ms.dirty, false) { // before idling SToplog
				return
			}
		}
		if !ok {
			select {
			case <-ctx.Done():
				return
			case op, ok = <-oplog:
				if !ok {
					return
				}
			}
		}
		// log.Tracef("got oplog %s", getOpName(op))
		// we deal with the same db all the time,
		// it is enough to dispatch based on collName only
		collName := getOpColl(op)
		if collName == "" {
			continue
		}

		if startAfter, ok := collSyncId[collName]; ok {
			syncId := getSyncId(op)
			if syncId <= startAfter.SyncId {
				log.Tracef("skipping %s due %s <= %s", collName, syncId, startAfter.SyncId)
				continue
			}
			delete(collSyncId, collName)
		}
		// check if it is subject to sync
		config, rt := ms.collMatch(collName)
		if rt || config == nil {
			continue // ignore unknown and RT collection
		}
		// now redirect handling of op to the channel for that collection
		collChan := ms.getCollChan(ctx, collName, config, rt)
		if CancelSend(ctx, collChan, op) {
			return
		}
	}
}
