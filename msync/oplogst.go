package mongosync

import (
	"context"
	"encoding/hex"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
)

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

type collSyncP struct {
	SyncId   string   `bson:"_id"`      // syncId to restore syncing
	CollName string   `bson:"collName"` // Coll which had been flushed
	Pending  []string `bson:"pending"`  // pending collections at the time of flushing
}

// getCollSyncId returns for each collection sync_id to start follow changes, it also returns minSyncId
// if collection is not a key, it was not synced before
func (ms *MongoSync) getCollSyncId(ctx context.Context) (minSyncId string, err error) {
	collName := ms.syncId.Database().Name() + "." + ms.syncId.Name()
	ms.collSyncId = make(map[string]string) // init
	// fetch CollSyncId records
	// Sort by `sync_id` field descending
	findOptions := options.Find().SetSort(bson.D{{"_id", -1}})
	cur, err := ms.syncId.Find(ctx, allRecords, findOptions)
	if err != nil {
		return "", fmt.Errorf("getCollSyncId: can't read from %s : %w", ms.syncId.Name(), err)
	}
	var documents []collSyncP
	if err := cur.All(ctx, &documents); err != nil {
		return "", fmt.Errorf("getCollSyncId: can't fetch documents from sender.%s : %w", ms.syncId.Name(), err)
	}
	if len(documents) == 0 {
		return "", nil
	}

	// now proceed from top to bottom trying whether we can start from
	maxId := documents[0].SyncId
	// pending are marked to look for the earlier records with this coll to start with
	pending := make(map[string]struct{})
	var purgeIds []string
	for _, d := range documents {
		_, collFound := ms.collSyncId[d.CollName] // getCollSyncId, init
		if !collFound {
			_, isPending := pending[d.CollName]
			var syncId string
			if isPending {
				// shall start syncing from here as buffer has not been flushed since here
				syncId = d.SyncId
				// check whether we can restore from here
				_, err = getChangeStream(ctx, ms.Sender, syncId)
				if err != nil {
					break
				}
				minSyncId = syncId
				delete(pending, d.CollName)
			} else {
				syncId = maxId
			}
			ms.collSyncId[d.CollName] = syncId // getCollSyncId, init
		}
		// make pending all of them who does not have SyncId
		newPending := false
		for _, coll := range d.Pending {
			_, hasSyncId := ms.collSyncId[coll] // getCollSyncId, init
			_, alreadyPending := pending[coll]
			if !hasSyncId && !alreadyPending {
				newPending = true
				pending[coll] = struct{}{}
			}
		}
		if collFound && !newPending {
			purgeIds = append(purgeIds, d.SyncId)
		}
	}
	// purge unnecessary records
	var orFilter []interface{}
	// https://mongoplayground.net/p/entNSEJPJgw
	// purge all that is less than minSyncId or in the purgeIds list
	if minSyncId != "" {
		orFilter = append(orFilter, bson.M{"name": bson.M{"$lt": minSyncId}})
	}
	if len(purgeIds) != 0 {
		orFilter = append(orFilter, bson.M{"name": bson.M{"$in": purgeIds}})
	}
	if len(orFilter) > 0 {
		dr, err := ms.syncId.DeleteMany(ctx, bson.M{"$or": orFilter})
		if err != nil {
			log.Errorf("faield to purge bookmark records %s", collName)
		} else {
			log.Infof("purged %d items bookmark records from %s", dr.DeletedCount, collName)
		}
	} else {
		_ = ms.syncId.Drop(ctx)
	}
	return minSyncId, nil
}

// initSTOplog finds out minimal sync_id from collMSync collection.
// which it can successfully resume oplogST watch.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplogST
// and returns empty collSyncId
func (ms *MongoSync) initSTOplog(ctx context.Context) {
	minId := ms.SyncCollections(ctx)
	oplog, err := ms.getOplog(ctx, ms.Sender, minId)
	if err != nil {
		log.Fatalf("failed to restore oplog for ST ops: %s", err)
	}
	ms.routines.Add(1) // go ms.runSToplog
	go ms.runSToplog(ctx, oplog)
	return
}

// runSToplog handles incoming oplogST entries from oplogST channel.
// it calls getCollChan to redirect oplog to channel for an ST collection.
// If SyncId for the collection is greater than current syncId - it skips op
func (ms *MongoSync) runSToplog(ctx context.Context, oplog Oplog) {
	defer ms.routines.Done() // runSToplog
	// loop until context tells we are done
	log.Trace("running SToplog")
	for {
		if len(oplog) == 0 {
			ms.dirty <- false
		}
		var op bson.Raw
		select {
		case op = <-oplog:
		case <-ctx.Done():
			return
		}
		log.Tracef("got oplog %s", getOpName(op))
		// we deal with the same db all the time,
		// it is enough to dispatch based on collName only
		collName := getOpColl(op)
		if collName == "" {
			continue
		}
		// check if it is subject to sync
		config, rt := ms.collMatch(collName)
		if rt || config == nil {
			continue // ignore unknown and RT collection
		}
		// check whether we reached syncId to start collection sync
		startFrom, ok := ms.collSyncId[collName] // monopoly by oplog
		if ok {
			syncId := getSyncId(op)
			if syncId < startFrom {
				log.Tracef("oplog skipped %s as %s < %s", getOpName(op), syncId, startFrom)
				continue
			}
			log.Infof("follow oplog for coll %s from %s, exchange %s", collName, syncId, ms.Name())
			delete(ms.collSyncId, collName) // monopoly by oplog
		}
		// now redirect handling of op to the channel for that collection
		ch := ms.getCollChan(ctx, collName, config, rt)
		ch <- op
	}
}
