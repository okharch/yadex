package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func fetchIds(ctx context.Context, coll *mongo.Collection) []interface{} {
	opts := options.Find().SetProjection(bson.D{{"_id", 1}})
	var recs []bson.D
	cursor, err := coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Errorf("Failed to fetch _id(s) from %s: %s", coll.Name(), err)
		return nil
	}
	if err := cursor.All(ctx, &recs); err != nil {
		log.Errorf("Failed to fetch _id(s) from %s: %s", coll.Name(), err)
		return nil
	}
	result := make([]interface{}, len(recs))
	for i, v := range recs {
		result[i] = v[0].Value
	}
	return result
}

// syncCollection
// insert all the docs from sender but those which _ids found at receiver
func (ms *MongoSync) syncCollection(ctx context.Context, collData *CollData, syncId chan string) (collSyncId string, err error) {
	maxBulkCount := collData.Config.Batch
	collName := collData.CollName
	log.Infof("cloning collection %s...", collName)
	var models []mongo.WriteModel
	totalBytes := 0
	if collData.OplogClass != OplogStored {
		panic("should be an ST collection!")
	}
	flush := func() {
		if len(models) == 0 {
			return
		}
		//getLock(&collData.RWMutex, false, collData.CollName)
		//releaseLock(&collData.RWMutex, false, collData.CollName)
		log.Tracef("syncCollection flushing %d records", len(models))
		bwOp := &BulkWriteOp{
			Coll:       collName,
			OplogClass: OplogStored,
			OpType:     OpLogUnordered,
			Models:     models,
			TotalBytes: totalBytes,
			CollData:   collData,
		}
		if ctx.Err() != nil {
			return
		}
		ms.bulkWriteST <- bwOp
		//_ = CancelSend(ctx, ms.bulkWriteST, bwOp, "BulkWriteST")
		totalBytes = 0
		models = nil
	}
	// copy all the records which are not on the receiver yet
	coll := ms.Sender.Collection(collName)
	receiverIds := fetchIds(ctx, ms.Receiver.Collection(collName))
	filter := bson.M{}
	if len(receiverIds) > 0 {
		log.Infof("found %d documents at receiver's collection %s...", len(receiverIds), collName)
		filter = bson.M{"_id": bson.M{"$nin": receiverIds}}
	}
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		log.Errorf("clone collection %s: can't read from sender: %s", collName, err)
	}
	collSyncId = GetState(syncId) // remember syncId before copying
	for cursor.Next(ctx) {
		if ctx.Err() != nil {
			return
		}
		l := len(cursor.Current)
		if totalBytes+l >= maxBulkCount {
			flush()
		}
		totalBytes += l
		models = append(models, &mongo.InsertOneModel{Document: cursor.Current})
	}
	if totalBytes > 0 {
		flush()
	}
	ms.WriteCollBookmark(ctx, collData, collSyncId)
	log.Infof("sync of %s coll completed at %s", collName, collSyncId)
	return
}

// SyncCollections checks for all ST collections from database
// whether they should be synced at all
// Before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) (collBookmark map[string]string, minSyncId, maxSyncId string) {
	// here we clone those collections which does not have sync_id
	defer SendState(ms.SyncSTDone, true)
	SendState(ms.SyncSTDone, false)
	colls, err := ms.Sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		log.Errorf("critical error:failed to obtain collection list: %s", err)
		return
	}
	log.Tracef("runSToplog: creating list of colls: %d", len(colls))
	syncCollections := make(map[string]*CollData)
	for _, coll := range colls {
		collData := ms.getCollData(coll)
		if collData.OplogClass == OplogStored {
			syncCollections[coll] = collData
		}
	}
	if len(colls) == 0 { // SyncCollections: nothing to do here
		return
	}
	log.Trace("runSToplog: getSyncIdOplog")
	// oplog to follow running changes while we are syncing collections
	syncId, stopSyncIdOplog := ms.getSyncIdOplog(ctx)
	// get all collections from database and clone those without sync_id
	collBookmark, maxSyncId = ms.getCollBookMarks(ctx)
	if maxSyncId == "" {
		// we start syncing from the scratch,
		// need to get some fresh syncId for syncCollection
		// triggerUpdate puts latest SyncId from triggered changes to syncId (state) channel
		// because syncId follows the latest syncId
		ms.triggerUpdate(ctx)
	} else {
		SendState(syncId, maxSyncId)
	}
	copied := 0
	for pass := 1; true; pass++ {
		var updated bool
		updated, minSyncId, maxSyncId = ms.updateCollBookmarks(ctx, collBookmark)
		if pass > 1 {
			if !updated {
				return
			}
			log.Warnf("starting pass %d to catch up for the oplog tail: consider increasing oplog size!", pass)
		}
		// iterate over all collections, sync those not bookmarked
		for coll, collData := range syncCollections {
			if ctx.Err() != nil {
				log.Debug("SyncCollections gracefully shutdown on cancelled context")
				stopSyncIdOplog()
				return
			}
			// we copy only ST collections which do not have a bookmark in collSyncId
			if _, bookmarked := collBookmark[coll]; bookmarked {
				continue
			}
			if syncId, err := ms.syncCollection(ctx, collData, syncId); err != nil {
				log.Errorf("sync collection %s for the exchange %s failed: %s", coll, ms.Name(), err)
			} else {
				collBookmark[coll] = syncId
				copied++
			}
		}
	}
	stopSyncIdOplog()
	log.Infof("finished syncing %d collections, %d were copied, restoring oplog from %v", len(syncCollections), copied, minSyncId)
	return
}

// it works during SyncCollections, it updates pending collections so BookMark collections is kept intact
func (ms *MongoSync) getSyncIdOplog(ctx context.Context) (syncId chan string, cancel context.CancelFunc) {
	syncId = make(chan string, 1)
	var eCtx context.Context
	eCtx, cancel = context.WithCancel(ctx)
	oplog, err := ms.getOplog(eCtx, ms.Sender, "", OplogSyncId)
	if err != nil {
		log.Fatalf("failed to get oplog, perhaps it is not activated: %s", err)
	}
	ms.routines.Add(1) // SyncId channel
	go func() {
		for op := range oplog {
			SendState(syncId, getSyncId(op))
			ms.ChangeColl <- ChangeColl{CollName: getOpColl(op), SyncId: getSyncId(op)}
		}
		ms.routines.Done() // SyncId channel
	}()
	return
}
