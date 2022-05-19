package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sort"
	"time"
	"yadex/config"
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
func (ms *MongoSync) syncCollection(ctx context.Context, collName string, maxBulkCount int, syncId chan string) (collSyncId string,
	updated time.Time, err error) {
	log.Infof("cloning collection %s...", collName)
	var models []mongo.WriteModel
	collSyncId = GetState(syncId) // remember syncId before copying
	updated = time.Now()
	totalBytes := 0
	flush := func() {
		totalBytes = 0
		if CancelSend(ctx, ms.dirty, true) { // syncCollection before putBwOp
			return
		}
		if len(models) == 0 {
			return
		}
		log.Tracef("syncCollection flushing %d records", len(models))
		ms.putBwOp(ctx, &BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models,
		})
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
	flush()
	ms.WriteCollBookmark(ctx, collName, collSyncId, updated)
	log.Infof("sync of %s coll completed", collName)
	return
}

func (ms *MongoSync) triggerUpdate(ctx context.Context, syncId chan string) {
	// trigger some change on sender to find out last SyncId
	tempColl := "rnd" + GetRandomHex(8)
	coll := ms.Sender.Collection(tempColl)
	// make some update to generate oplogRT
	log.Tracef("triggering update on %s in order to get SyncId", tempColl)
	if _, err := coll.InsertOne(ctx, bson.D{}); err != nil {
		log.Fatalf("failed to update sender in order to find out last SyncId: %s", err)
	}
	_ = coll.Drop(ctx)
	<-syncId // skip insert, will be restoring after drop
	sid := <-syncId
	SendState(syncId, sid) // get LastSyncId from the last op
}

func (ms *MongoSync) getSyncIdOplog(ctx context.Context) (syncId chan string, cancel context.CancelFunc) {
	syncId = make(chan string, 1)
	var eCtx context.Context
	eCtx, cancel = context.WithCancel(ctx)
	oplog, err := ms.getOplog(eCtx, ms.Sender, "", "LastSyncId")
	if err != nil {
		log.Fatalf("failed to get oplog, perhaps it is not activated: %s", err)
	}
	ms.routines.Add(1) // SyncId channel
	go func() {
		for op := range oplog {
			SendState(syncId, getSyncId(op))
		}
		ms.routines.Done() // SyncId channel
	}()
	return
}

// SyncCollections checks for all ST collections from database
// whether they should be synced at all
// Before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) (collBookmark map[string]*CollBookmark, minSyncId string,
	minTime, maxTime time.Time) {
	// here we clone those collections which does not have sync_id
	colls, err := ms.Sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		log.Fatalf("critical error:failed to obtain collection list: %s", err)
	}
	syncCollections := make(map[string]*config.DataSync)
	for _, coll := range colls {
		cfg, rt := ms.collMatch(coll)
		if !(rt || cfg == nil) {
			syncCollections[coll] = cfg
		}
	}
	// get all collections from database and clone those without sync_id
	syncId, cancel := ms.getSyncIdOplog(ctx) // oplog to follow running changes while we syncing collections
	var purgeIds []time.Time
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
		for coll, cfg := range syncCollections {
			if ctx.Err() != nil {
				log.Debug("SyncCollections gracefully shutdown on cancelled context")
				cancel()
				return
			}
			// we copy only ST collections which do not have a bookmark in collSyncId
			if _, bookmarked := collBookmark[coll]; bookmarked {
				continue
			}
			if syncId, updated, err := ms.syncCollection(ctx, coll, cfg.Batch, syncId); err != nil {
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
	cancel()
	if len(purgeIds) != 0 {
		ms.purgeBookmarks(ctx, purgeIds)
	}
	if CancelSend(ctx, ms.dirty, false) { // SyncCollections before exit
		return
	}
	log.Infof("finished syncing %d collections, %d were copied, restoring oplog from %v", len(syncCollections), copied, minTime)
	return
}

type CollBookmark = struct {
	Updated time.Time
	SyncId  string
}

// getCollBookMarks build map[coll]CollBookmark
// if collection is not a key, it was not synced before
func (ms *MongoSync) getCollBookMarks(ctx context.Context) (collBookmark map[string]*CollBookmark, purgeIds []time.Time) {
	collName := ms.syncId.Database().Name() + "." + ms.syncId.Name()
	// fetch CollSyncId records
	// Sort by `sync_id` field descending
	findOptions := options.Find().SetSort(bson.D{{"_id", -1}})
	cur, err := ms.syncId.Find(ctx, allRecords, findOptions)
	if err == mongo.ErrNoDocuments {
		return
	}
	if err != nil {
		log.Errorf("failed to fetch boomark info to follow oplog: %s, dropping %s will help", err, collName)
		return
	}
	var documents []collSyncP
	if err := cur.All(ctx, &documents); err != nil {
		log.Errorf("failed to fetch boomark info to follow oplog: %s, dropping %s will help", err, collName)
		return
	}
	if len(documents) == 0 {
		return
	}

	// now proceed from top to bottom trying whether we can start from
	maxId := ""
	noPending := false
	// pending are marked to look for the earlier records with this coll to start with
	pending := make(map[string]struct{})
	collBookmark = make(map[string]*CollBookmark)
	for _, d := range documents {
		if d.SyncId > maxId {
			// replace previous maxId with current
			for _, v := range collBookmark {
				if v.SyncId == maxId {
					v.SyncId = d.SyncId
				}
			}
			maxId = d.SyncId
		}
		_, collFound := collBookmark[d.CollName]
		if collFound {
			purgeIds = append(purgeIds, d.Updated)
		} else {
			if noPending {
				collBookmark[d.CollName] = &CollBookmark{d.Updated, maxId}
				continue
			}
			_, isPending := pending[d.CollName]
			if isPending {
				collBookmark[d.CollName] = &CollBookmark{d.Updated, d.SyncId}
			} else {
				collBookmark[d.CollName] = &CollBookmark{d.Updated, maxId}
			}
		}
		if noPending || len(d.Pending) == 0 {
			noPending = true
			continue
		}
		// d.Pending => pending[]
		pending = MakeSet(d.Pending)
	}
	return
}

func (ms *MongoSync) updateCollBookmarks(ctx context.Context, acollBookmark map[string]*CollBookmark,
	apurgeIds []time.Time) (updated bool, collBookmark map[string]*CollBookmark, purgeIds []time.Time,
	minSyncId, maxSyncId string, minTime, maxTime time.Time) {
	// now sort SyncIds
	syncId2Coll := make(map[string][]string)
	collBookmark = acollBookmark
	purgeIds = apurgeIds
	for coll, bm := range collBookmark {
		if bm.Updated.After(maxTime) {
			maxTime = bm.Updated
		}
		syncId2Coll[bm.SyncId] = append(syncId2Coll[bm.SyncId], coll)
	}
	syncIds := Keys(syncId2Coll)
	// sort them by ascending
	sort.Strings(syncIds)
	i := 0
	for i < len(syncIds) {
		if _, err := getChangeStream(ctx, ms.Sender, syncIds[i]); err == nil {
			minSyncId = syncIds[i]
			colls := syncId2Coll[minSyncId]
			for _, coll := range colls {
				if minTime.Before(collBookmark[coll].Updated) {
					minTime = collBookmark[coll].Updated
				}
			}
			log.Infof("found bookmark %s %v", colls[0], minTime)
			break
		}
		i++
	}

	if i == len(syncIds) {
		// it means we were not be able to restore from any bookmark, let's drop it
		_ = ms.syncId.Drop(ctx)
		collBookmark = make(map[string]*CollBookmark)
		purgeIds = nil
		updated = true
		return
	}

	maxSyncId = syncIds[len(syncIds)-1]

	// add to purge everything between 0 and i
	for j := 0; j < i; j++ {
		colls := syncId2Coll[syncIds[j]]
		for _, coll := range colls {
			purgeIds = append(purgeIds, collBookmark[coll].Updated)
			delete(collBookmark, coll)
		}
	}
	updated = i > 0
	return
}

func (ms *MongoSync) purgeBookmarks(ctx context.Context, purgeIds []time.Time) {
	filter := bson.M{"_id": bson.M{"$in": purgeIds}}
	if dr, err := ms.syncId.DeleteMany(ctx, filter); err != nil {
		log.Errorf("faield to purge bookmark records, exchange %s", ms.Name())
	} else {
		log.Infof("purged %d items bookmark records, exchange %s", dr.DeletedCount, ms.Name())
	}
}
