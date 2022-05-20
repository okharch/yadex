package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
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

func (ms *MongoSync) WriteCollBookmark(ctx context.Context, collData *CollData, SyncId string, updated time.Time) {
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
