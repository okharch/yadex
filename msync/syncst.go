package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
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
func (ms *MongoSync) syncCollection(ctx context.Context, collData *CollData, syncId chan string) (collSyncId string,
	updated primitive.DateTime, err error) {
	maxBulkCount := collData.Config.Batch
	collName := collData.CollName
	log.Infof("cloning collection %s...", collName)
	var models []mongo.WriteModel
	collSyncId = GetState(syncId) // remember syncId before copying
	updated = primitive.NewDateTimeFromTime(time.Now())
	totalBytes := 0
	if collData.OplogClass != OplogStored {
		panic("should be an ST collection!")
	}
	flush := func() {
		if len(models) == 0 {
			return
		}
		//getLock(&collData.RWMutex, false, collData.CollName)
		collData.Lock()
		collData.Dirty += totalBytes
		collData.Unlock()
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
	log.Infof("sync of %s coll completed", collName)
	return
}

// SyncCollections checks for all ST collections from database
// whether they should be synced at all
// Before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) (collBookmark map[string]*CollBookmark, minSyncId string,
	minTime, maxTime primitive.DateTime) {
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
	// get all collections from database and clone those without sync_id
	syncId, stopSyncIdOplog := ms.getSyncIdOplog(ctx) // oplog to follow running changes while we syncing collections
	var purgeIds []primitive.DateTime
	collBookmark, purgeIds = ms.getCollBookMarks(ctx)
	updated := true
	copied := 0
	var maxSyncId, lastSyncId string
	for updated {
		updated, collBookmark, purgeIds, minSyncId, maxSyncId, minTime, maxTime = ms.updateCollBookmarks(ctx, collBookmark, purgeIds)
		if lastSyncId == "" {
			if maxSyncId == "" {
				ms.triggerUpdate(ctx, syncId)
				lastSyncId = GetState(syncId)
			} else {
				lastSyncId = maxSyncId
				SendState(syncId, lastSyncId)
			}
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
			if syncId, updated, err := ms.syncCollection(ctx, collData, syncId); err != nil {
				log.Errorf("sync collection %s for the exchange %s failed: %s", coll, ms.Name(), err)
			} else {
				collBookmark[coll] = &CollBookmark{
					Updated: updated,
					SyncId:  syncId,
				}
				copied++
			}
		}
	}
	stopSyncIdOplog()
	if len(purgeIds) != 0 {
		ms.purgeBookmarks(ctx, purgeIds)
	}
	log.Infof("finished syncing %d collections, %d were copied, restoring oplog from %v", len(syncCollections), copied, minTime)
	return
}
