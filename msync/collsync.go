package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
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
// 1. insert all the docs with creationTime > lastSyncTime or creationTime is null unordered
// 2. update all the docs with lastTimeModifed > creationTime & lastSyncTime
// 3. compare all the docs without lastTimeModifed and sy
func syncCollection(ctx context.Context, sender, receiver *mongo.Database, collName string, maxBulkCount int64,
	putOp putBulkWriteOp) error {
	log.Infof("cloning collection %s...", collName)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var wg *sync.WaitGroup
	models := make([]mongo.WriteModel, maxBulkCount)
	count := int64(0)
	flush := func() {
		if count == 0 {
			return
		}
		wg = putOp(&BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models[:count],
			//SyncId: "",
			//RealTime: false,
		})
		models = make([]mongo.WriteModel, maxBulkCount)
		count = 0
	}
	// copy all the records which are not on the receiver yet
	coll := sender.Collection(collName)
	receiverIds := fetchIds(ctx, receiver.Collection(collName))
	filter := bson.M{}
	if len(receiverIds) > 0 {
		log.Infof("found %d documents at receiver's collection %s...", len(receiverIds), collName)
		filter = bson.M{"_id": bson.M{"$nin": receiverIds}}
	}
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("ErrCloneCollection: can't read from sender.%s : %w", collName, err)
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
	wg.Wait()
	return ctx.Err()
}

// SyncCollections checks for all collections from database whether they should be synced at all using delay != -1 flag
// then it fetches all _ids from the receivers's collection to avoid resending records with those ids
// before sending all new records to sender it remembers syncId for the collection at sender
// it returns nil if it was able to clone all the other collections successfuly into chBulkWriteOps channels
func SyncCollections(ctx context.Context, collMatch CollMatch, collSyncStartFrom map[string]string, sender, receiver *mongo.Database,
	putOp putBulkWriteOp) error {
	// here we clone those collections which does not have sync_id
	// get all collections from database and clone those without sync_id
	colls, err := sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		return err
	}
	for _, coll := range colls {
		delay, batch, rt := collMatch(coll)
		if delay == -1 || rt {
			continue
		}
		// we copy only ST collections
		if _, ok := collSyncStartFrom[coll]; !ok {
			lastSyncId, err := getLastSyncId(ctx, sender)
			if err != nil {
				return fmt.Errorf("Can't fetch SyncId to start replication after collection %s clone: %w", coll, err)
			}
			if err := syncCollection(ctx, sender, receiver, coll, batch, putOp); err != nil {
				return fmt.Errorf("failed to clone collection %s to receiver: %w", coll, err)
			}
			collSyncStartFrom[coll] = lastSyncId
		}
	}
	return nil
}
