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
	err error) {
	log.Infof("cloning collection %s...", collName)
	var models []mongo.WriteModel
	totalBytes := 0
	flush := func() {
		totalBytes = 0
		ms.dirty <- true // syncCollection before putBwOp
		if len(models) == 0 {
			return
		}
		log.Tracef("syncCollection flushing %d records", len(models))
		ms.putBwOp(&BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models,
			SyncId: "",
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
	collSyncId = GetState(syncId)
	ms.WriteCollBookmark(ctx, collName, collSyncId)
	log.Infof("sync of %s coll completed", collName)
	return
}

func (ms *MongoSync) getSyncIdOplog(ctx context.Context) (syncId chan string, cancel context.CancelFunc) {
	syncId = make(chan string, 1)
	var eCtx context.Context
	eCtx, cancel = context.WithCancel(ctx)
	oplog, err := ms.getOplog(eCtx, ms.Sender, "", time.Now(), "LastSyncId")
	if err != nil {
		log.Fatalf("failed to get oplog, perhaps it is not activated: %s", err)
	}
	time.Sleep(time.Millisecond * 10) // wait for oplog to be created
	// trigger some change on sender to find out last SyncId
	coll := ms.Sender.Collection("rnd" + GetRandomHex(8))
	// make some update to generate oplogRT
	if _, err := coll.InsertOne(ctx, bson.D{}); err != nil {
		log.Fatalf("failed to update sender in order to find out last syncId: %s", err)
	}
	_ = coll.Drop(ctx)
	ms.routines.Add(1) // syncId channel
	go func() {
		for op := range oplog {
			SendState(syncId, getSyncId(op))
		}
		ms.routines.Done() // syncId channel
	}()
	return
}

// SyncCollections checks for all ST collections from database
// whether they should be synced at all
// Before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) (collSynced map[string]string, minSyncId string, minTime time.Time) {
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
	collBookmark, purgeIds := ms.getCollBM(ctx)
	updated := true
	copied := 0
	collSynced = make(map[string]string)
	for updated {
		updated, minSyncId, minTime = ms.UpdateBookmarks(ctx, collBookmark, purgeIds)
		for coll, cfg := range syncCollections {
			if ctx.Err() != nil {
				log.Debug("SyncCollections gracefully shutdown on cancelled context")
				break
			}
			if _, synced := collSynced[coll]; synced {
				continue
			}
			if _, bookmarked := collBookmark[coll]; bookmarked {
				continue
			}
			// we copy only ST collections which do not have a bookmark in collSyncId
			if syncId, err := ms.syncCollection(ctx, coll, cfg.Batch, syncId); err != nil {
				log.Errorf("sync collection %s for the exchange %s failed: %s", coll, ms.Name(), err)
			} else {
				collSynced[coll] = syncId
				copied++
			}
		}
	}
	cancel()
	for coll, bm := range collBookmark {
		collSynced[coll] = bm.syncId
	}
	if len(purgeIds) != 0 {
		ms.purgeBookmarks(ctx, purgeIds)
	}
	ms.dirty <- false // SyncCollections before exit
	log.Infof("finished syncing %d collections, %d were copied, restoring oplog from %v", len(syncCollections), copied, minTime)
	return
}

type BM = struct {
	updated time.Time
	syncId  string
}

// getCollBM build map[coll]BM
// if collection is not a key, it was not synced before
func (ms *MongoSync) getCollBM(ctx context.Context) (collBookmark map[string]*BM, purgeIds []time.Time) {
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
	collBookmark = make(map[string]*BM)
	for _, d := range documents {
		if d.SyncId > maxId {
			// replace previous maxId with current
			for _, v := range collBookmark {
				if v.syncId == maxId {
					v.syncId = d.SyncId
				}
			}
			maxId = d.SyncId
		}
		_, collFound := collBookmark[d.CollName]
		if collFound {
			purgeIds = append(purgeIds, d.Updated)
		} else {
			if noPending {
				collBookmark[d.CollName] = &BM{d.Updated, maxId}
				continue
			}
			_, isPending := pending[d.CollName]
			if isPending {
				collBookmark[d.CollName] = &BM{d.Updated, d.SyncId}
			} else {
				collBookmark[d.CollName] = &BM{d.Updated, maxId}
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

func (ms *MongoSync) UpdateBookmarks(ctx context.Context, collBookmark map[string]*BM, purgeIds []time.Time) (updated bool,
	minSyncId string, minTime time.Time) {
	// now sort SyncIds
	syncId2Coll := make(map[string][]string)
	for coll, bm := range collBookmark {
		syncId2Coll[bm.syncId] = append(syncId2Coll[bm.syncId], coll)
	}
	syncIds := Keys(syncId2Coll)
	// sort them by ascending
	sort.Strings(syncIds)
	i := 0
	for i < len(syncIds) {
		if _, err := getChangeStream(ctx, ms.Sender, syncIds[i]); err == nil {
			minSyncId = syncIds[i]
			colls := syncId2Coll[minSyncId]
			minTime = collBookmark[colls[0]].updated
			log.Infof("Bookmark %s %v %s", colls[0], minTime, minSyncId)
			break
		}
		i++
	}
	// add to purge everything between 0 and i
	for j := 0; j < i; j++ {
		colls := syncId2Coll[syncIds[j]]
		for _, coll := range colls {
			purgeIds = append(purgeIds, collBookmark[coll].updated)
			delete(collBookmark, coll)
		}
	}
	updated = i > 0
	return
}

func (ms *MongoSync) purgeBookmarks(ctx context.Context, purgeIds []time.Time) {
	if len(purgeIds) == 0 {
		return
	}
	filter := bson.M{"_id": bson.M{"$in": purgeIds}}
	if dr, err := ms.syncId.DeleteMany(ctx, filter); err != nil {
		log.Errorf("faield to purge bookmark records, exchange %s", ms.Name())
	} else {
		log.Infof("purged %d items bookmark records, exchange %s", dr.DeletedCount, ms.Name())
	}
}
