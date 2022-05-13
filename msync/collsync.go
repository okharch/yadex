package mongosync

import (
	"context"
	"fmt"
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
func (ms *MongoSync) syncCollection(ctx context.Context, collName string, maxBulkCount int) error {
	log.Infof("cloning collection %s...", collName)
	models := make([]mongo.WriteModel, maxBulkCount)
	count := 0
	var syncId string
	flush := func() {
		if count == 0 {
			return
		}
		log.Tracef("syncCollection flushing %d records", len(models))
		syncId = ms.getLastSyncId()
		ms.putBwOp(&BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models[:count],
			SyncId: syncId,
		})
		models = make([]mongo.WriteModel, maxBulkCount)
		count = 0
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
		return fmt.Errorf("clone collection %s: can't read from sender: %w", collName, err)
	}
	for cursor.Next(ctx) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		models[count] = &mongo.InsertOneModel{Document: cursor.Current}
		count++
		if count == maxBulkCount {
			flush()
		}
	}
	flush()
	log.Infof("sync of %s completed", collName)
	ms.Lock()
	ms.collSyncId[collName] = syncId
	ms.Unlock()
	return ctx.Err()
}

// SyncCollections checks for all collections from database
// whether they should be synced at all using delay != -1 flag
// Then before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) {
	// here we clone those collections which does not have sync_id
	// get all collections from database and clone those without sync_id
	colls, err := ms.Sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		log.Errorf("critical error:failed to obtain collection list: %s", err)
		return
	}
	for _, coll := range colls {
		config, rt := ms.collMatch(coll)
		if config == nil || rt {
			continue
		}
		// we copy only ST collections which do not have a bookmark in collSyncId
		if _, ok := ms.collSyncId[coll]; !ok {
			if err != nil {
				log.Errorf("Can't fetch SyncId to start replication after collection %s clone: %s: make sure oplog is on", coll, err)
				return // this is fatal
			}
			if err := ms.syncCollection(ctx, coll, config.Batch); err != nil {
				log.Errorf("sync collection %s for the exchange %s failed: %s", coll, ms.Name(), err)
			}
		}
	}
	return
}
