package mongosync

import (
	"context"
	"encoding/hex"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"sort"
	"time"
)

type SyncBookmark = time.Time

var ZeroBookmark SyncBookmark

type CollSyncBookmark struct {
	SyncId   string       `bson:"sync_id"`  // SyncId to restore syncing
	CollName string       `bson:"collName"` // Coll which had been flushed
	Pending  []string     `bson:"pending"`  // pending collections at the time of flushing
	Id       SyncBookmark `bson:"_id"`      // time is used, hopefully ST collection is written one at a time,
	// hopefully it will be protection against duplicate key
}

func (ms *MongoSync) WriteCollBookmark(ctx context.Context, collData *CollData, SyncId string, updated SyncBookmark) {
	var pending []string
	for collName, collData := range ms.collData { // get pending collections: collData.Dirty > 0
		collData.RLock()
		if collData.Dirty > 0 {
			pending = append(pending, collName)
		}
		collData.RUnlock()
	}
	collName := collData.CollName
	doc := CollSyncBookmark{Id: updated, SyncId: SyncId, CollName: collName, Pending: pending}
	// purge previous bookmarks
	collData.Lock()
	defer collData.Unlock()
	if collData.LastSyncId > SyncId {
		// do not write this bookmark as more mature has been written before
		return
	}
	collData.LastSyncId = SyncId
	if _, err := ms.syncId.InsertOne(ctx, &doc); err != nil {
		log.Errorf("failed to update sync_id for %s: %s", collName, err)
		return
	}
	if collData.PrevBookmark != ZeroBookmark {
		if _, err := ms.syncId.DeleteOne(ctx, bson.M{"_id": collData.PrevBookmark}); err != nil {
			log.Errorf("failed to clean sync_id(bookmark) for %s(%v): %s", collName, collData.PrevBookmark, err)
		} else {
			log.Tracef("purged previous bookmark for %s(%v)", collName, collData.PrevBookmark)
		}
	}
}

type CollBookmark = struct {
	Updated SyncBookmark
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
	var documents []CollSyncBookmark
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
			purgeIds = append(purgeIds, d.Id)
		} else {
			if noPending {
				collBookmark[d.CollName] = &CollBookmark{d.Id, maxId}
				continue
			}
			_, isPending := pending[d.CollName]
			if isPending {
				collBookmark[d.CollName] = &CollBookmark{d.Id, d.SyncId}
			} else {
				collBookmark[d.CollName] = &CollBookmark{d.Id, maxId}
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

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
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
