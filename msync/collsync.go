package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
func (ms *MongoSync) syncCollection(ctx context.Context, collName string, maxBulkCount int, syncId chan string) error {
	log.Infof("cloning collection %s...", collName)
	models := make([]mongo.WriteModel, maxBulkCount)
	count := 0
	totalBytes := 0
	flush := func() {
		if count == 0 {
			return
		}
		log.Tracef("syncCollection flushing %d records", len(models))
		ms.putBwOp(&BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models[:count],
			SyncId: GetState(syncId),
		})
		models = make([]mongo.WriteModel, maxBulkCount)
		count = 0
		totalBytes = 0
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
		l := len(cursor.Current)
		if totalBytes+l >= maxBulkCount {
			flush()
		}
		models[count] = &mongo.InsertOneModel{Document: cursor.Current}
		totalBytes += l
		count++
	}
	flush()
	log.Infof("sync of %s coll completed", collName)
	return ctx.Err()
}

func (ms *MongoSync) initSyncId(ctx context.Context) (chan string, context.CancelFunc) {
	oplogCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	oplog, err := ms.getOplog(oplogCtx, ms.Sender, "")
	if err != nil {
		log.Fatalf("failed to get oplog, perhaps it is not activated: %s", err)
	}
	syncId := make(chan string, 1)
	ms.routines.Add(1) // SyncId chan
	go func() {
		ms.routines.Done() // SyncId chan
		for op := range oplog {
			SendState(syncId, getSyncId(op))
		}
		close(syncId)
	}()
	// trigger some change on sender to find out last SyncId
	coll := ms.Sender.Collection("rnd" + GetRandomHex(8))
	// make some update to generate oplogRT
	if _, err := coll.InsertOne(ctx, bson.D{}); err != nil {
		log.Fatalf("failed to update sender in order to find out last syncId: %s", err)
	}
	return syncId, cancel
}

// SyncCollections checks for all ST collections from database
// whether they should be synced at all
// Before calling syncCollection for a collection it remembers last SyncId,
// so it can start dealing with oplogST starting with that Id
// it returns nil if it was able to clone all the collections
// successfully into chBulkWriteOps channels
func (ms *MongoSync) SyncCollections(ctx context.Context) (minId string) {
	// here we clone those collections which does not have sync_id
	// get all collections from database and clone those without sync_id
	syncId, cancelSyncId := ms.initSyncId(ctx) // channel which broadcasts syncId for synced collections to follow
	minId, err := ms.getCollSyncId(ctx)
	if err != nil {
		log.Errorf("failed to restore from oplog by bookmarks: %s", err)
		return ""
	}
	colls, err := ms.Sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		log.Fatalf("critical error:failed to obtain collection list: %s", err)
	}
	syncCollections := make(map[string]*config.DataSync)
	for _, coll := range colls {
		config, rt := ms.collMatch(coll)
		_, canRestore := ms.collSyncId[coll] // before SyncCollection, init phase
		if canRestore && config != nil && !rt {
			syncCollections[coll] = config
		}
	}
	ms.routines.Add(1) // SyncCollections
	go func() {
		for coll, config := range syncCollections {
			if ctx.Err() != nil {
				log.Debug("SyncCollections gracefully shutdown on cancelled context")
				break
			}
			// we copy only ST collections which do not have a bookmark in collSyncId
			if err := ms.syncCollection(ctx, coll, config.Batch, syncId); err != nil {
				log.Errorf("sync collection %s for the exchange %s failed: %s", coll, ms.Name(), err)
			}
		}
		cancelSyncId()
		ms.routines.Done() // SyncCollections
	}()
	return minId
}
