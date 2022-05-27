package mongosync

import (
	"context"
	"encoding/hex"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/exp/maps"
	"math/rand"
	"sort"
)

// Bookmark keeps Data needed to continue syncing ST collections
// For each collection it keeps single record (CollName, SyncId, Pending)
// CollName is a primary key, and the latest SyncId for this collection is stored.
// If there were pending changes for other collections at the moment of writing,
// it stores the oldest sync id, which has not been written yet for the those collections.
// It is for making sure that we continue syncing from the changes which have not been committed yet.
// It uses (CollName,SyncId) information to ignore changes that were already transferred to the receiver.
// But overall we start from the oldest pending SyncId

type CollSyncBookmark struct {
	CollName string `bson:"_id"`    // CollName is primary key
	SyncId   string `bson:"SyncId"` // the Latest SyncId commited for ST collection. Used to restore syncing
	// pending collections at the time of BulkWrite. If list is empty - there were not pending collections
	Pending map[string]string `bson:"Pending,omitempty"`
}

func (ms *MongoSync) WriteCollBookmark(ctx context.Context, collData *CollData, SyncId string) {
	collName := collData.CollName

	// find out pending and clear entries for collName with lesser SyncId
	ms.ChangeColl <- ChangeColl{CollName: collName, SyncId: SyncId, BulkWrite: true}
	pending := <-ms.Pending

	// this is the update record for MongoDB collection
	update := CollSyncBookmark{CollName: collName, SyncId: SyncId, Pending: pending}
	filter := bson.M{"_id": collName}

	// upsert the record
	opts := options.Update().SetUpsert(true)
	if ur, err := ms.syncId.UpdateOne(ctx, filter, update, opts); err != nil {
		log.Errorf("failed to update sync_id for %s: %s", collName, err)
		return
	} else {
		if ur.MatchedCount > 0 {
			log.Tracef("updated bookmark %+v", update)
		} else {
			log.Tracef("added bookmark %+v", update)
		}
	}
}

// getCollBookMarks returns  the map CollName=>SyncId which it builds using bookmark MongoDB collection
// if collection is not stored in the DB, it was not synced before
// It will be synced in by syncCollection first, and then continued from the after maxSyncId
// otherwise it looks for the record with the latest written SyncId (sort by SyncId desc)
// If there were not pending collections in that record, the syncing starts from the latest SyncId
// for all the collections stored in Bookmark collections.
// Otherwise it fixes SyncId for the pending collections and their syncing will be resumed from that SyncId
// All other collections will start syncing still from the maxSyncId
func (ms *MongoSync) getCollBookMarks(ctx context.Context) (collBookmark map[string]string, maxSyncId string) {
	collBookmark = make(map[string]string)
	collName := ms.syncId.Database().Name() + "." + ms.syncId.Name()
	// fetch CollSyncId records
	// Sort by `sync_id` field descending
	findOptions := options.Find().SetSort(bson.D{{"SyncId", -1}})
	cur, err := ms.syncId.Find(ctx, allRecords, findOptions)
	if err == mongo.ErrNoDocuments {
		return
	}
	if err != nil {
		log.Errorf("failed to fetch boomark info to follow oplog: %s, dropping %s will help", err, collName)
		return
	}
	var documents []CollSyncBookmark
	if err := cur.All(ctx, &documents); err != nil {
		log.Errorf("failed to fetch boomark info to follow oplog: %s, dropping %s will help", err, collName)
		return
	}
	if len(documents) == 0 {
		return
	}
	// get the latest record (sorted by SyncId desc)
	pending := documents[0].Pending
	maxSyncId = documents[0].SyncId
	for _, d := range documents {
		if pending, ok := pending[d.CollName]; ok {
			collBookmark[d.CollName] = pending
		} else {
			collBookmark[d.CollName] = maxSyncId
		}
	}
	return
}

func (ms *MongoSync) updateCollBookmarks(ctx context.Context, collBookmark map[string]string) (updated bool, minSyncId, maxSyncId string) {
	// now sort SyncIds
	if len(collBookmark) == 0 {
		return
	}
	syncId2Colls := make(map[string][]string)
	for coll, bm := range collBookmark {
		syncId2Colls[bm] = append(syncId2Colls[bm], coll)
	}
	syncIds := maps.Keys(syncId2Colls)
	maxSyncId = syncIds[len(syncIds)-1]
	// sort them by ascending
	sort.Strings(syncIds)
	start := -1
	for i, syncId := range syncIds {
		if _, err := getChangeStream(ctx, ms.Sender, syncId); err == nil {
			start = i
			minSyncId = syncId
			break
		}
	}
	if start == 0 {
		return
	}
	if start == -1 {
		// drop all bookmarks
		maps.Clear(collBookmark)
		err := ms.syncId.Drop(ctx)
		if err != nil {
			log.Errorf("failed to drop bookmarks: %s", err)
		}
		updated = true
		return
	}
	var purgeBMs []string
	for _, syncId := range syncIds[:start] {
		purgeBMs = append(purgeBMs, syncId2Colls[syncId]...)
	}
	updated = len(purgeBMs) > 0
	if !updated {
		return
	}
	for _, coll := range purgeBMs {
		delete(collBookmark, coll)
	}
	ms.purgeBookmarks(ctx, purgeBMs)
	return
}

func (ms *MongoSync) purgeBookmarks(ctx context.Context, purgeIds []string) {
	filter := bson.M{"_id": bson.M{"$in": purgeIds}}
	if dr, err := ms.syncId.DeleteMany(ctx, filter); err != nil {
		log.Errorf("faield to purge bookmark records, exchange %s", ms.Name())
	} else {
		log.Infof("purged %d items bookmark records, exchange %s", dr.DeletedCount, ms.Name())
	}
}

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

// triggerUpdate triggers some change on sender to find out last SyncId
func (ms *MongoSync) triggerUpdate(ctx context.Context) {
	tempColl := "rnd" + GetRandomHex(8)
	coll := ms.Sender.Collection(tempColl)
	// make some update to generate oplogRT
	log.Tracef("triggering update on %s in order to get SyncId", tempColl)
	if _, err := coll.InsertOne(ctx, bson.D{}); err != nil {
		log.Fatalf("failed to update sender in order to find out last SyncId: %s", err)
	}
	_ = coll.Drop(ctx)
}
