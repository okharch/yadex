package mongosync // getOpLog gets oplog and decodes it to bson.M. If it fails it returns nil
import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"time"
)

func getOpLog(ctx context.Context, changeStream *mongo.ChangeStream) (bson.M, error) {
	var op bson.M
	if changeStream.Next(ctx) && changeStream.Decode(&op) == nil {
		return op, nil
	}
	return nil, changeStream.Err()
}

// getChangeStream tries to resume sync from last successfully committed syncId.
// That id is stored for each mongo collection each time after successful write oplog into receiver database.
func getChangeStream(ctx context.Context, sender *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	opts.SetMaxAwaitTime(time.Millisecond * 50)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplog before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}

func (ms *MongoSync) GetDbOpLog(ctx context.Context, db *mongo.Database, syncId string) (<-chan bson.Raw, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw)
	go func() {
		defer close(ch)
		for {
			success := changeStream.TryNext(ctx)
			if !success {
				ms.idleChan <- true
				success = changeStream.Next(ctx)
			}
			ms.idleChan <- false
			if success {
				ch <- changeStream.Current
			}
			err = changeStream.Err()
			if err == nil {
				continue
			}
			if errors.Is(err, context.Canceled) {
				log.Info("Process oplog gracefully shutdown")
			} else {
				log.Errorf("shutdown oplog : can't get next oplog entry %s", err)
			}
			return
		}
	}()
	return ch, nil
}

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

// getLastSyncId retrieves where oplog is now.
// It inserts some record into random collection which it then drops
// it returns syncIid for insert operation.
func getLastSyncId(ctx context.Context, sender *mongo.Database) (string, error) {
	coll := sender.Collection("rnd" + GetRandomHex(8))
	changeStreamLastSync, err := getChangeStream(ctx, sender, "")
	if err != nil {
		return "", err
	}
	defer func() {
		// drop the temporary collection
		_ = changeStreamLastSync.Close(ctx)
		_ = coll.Drop(ctx)
	}()
	// make some update to generate oplog
	if _, err := coll.InsertOne(ctx, bson.M{"DirtyDocs": 1}); err != nil {
		return "", err
	}
	op, err := getOpLog(ctx, changeStreamLastSync)
	if op == nil {
		return "", err
	}
	return op["_id"].(bson.M)["_data"].(string), nil
}

type CollSync struct {
	SyncId   string    `bson:"sync_id"`
	CollName string    `bson:"_id"`
	Updated  time.Time `bson:"updated"`
}

// fetchCollSyncId fetches sync_id from bookmarkCollSyncId(sync_id, collection_name) sorted by sync_id
func fetchCollSyncId(ctx context.Context, bookmarkCollSyncId *mongo.Collection) (documents []CollSync, err error) {
	findOptions := options.Find()
	// Sort by `sync_id` field ascending
	findOptions.SetSort(bson.D{{"sync_id", 1}})
	cur, err := bookmarkCollSyncId.Find(ctx, allRecords, findOptions)
	if err != nil {
		return nil, fmt.Errorf("fetchCollSyncId: can't read from %s : %w", bookmarkCollSyncId.Name(), err)
	}
	if err := cur.All(ctx, &documents); err != nil {
		return nil, fmt.Errorf("fetchCollSyncId: can't fetch documents from sender.%s : %w", bookmarkCollSyncId.Name(), err)
	}
	return documents, nil
}

// resumeOplog finds out minimal sync_id from collMSync collection.
// which it can successfully resume oplog watch.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplog
// and returns empty collSyncId
func (ms *MongoSync) resumeOplog(ctx context.Context) error {
	csBookmarks, err := fetchCollSyncId(ctx, ms.CollSyncId)
	if ctx.Err() != nil {
		return ctx.Err() // gracefully handle sync.Stop
	}
	if err != nil {
		log.Warn(err)
	}
	ms.collSyncId = make(map[string]string, len(csBookmarks))
	for _, b := range csBookmarks {
		if ms.oplog == nil {
			ms.oplog, err = ms.GetDbOpLog(ctx, ms.Sender, b.SyncId)
			if ms.oplog != nil {
				log.Infof("sync was resumed from (%s) %s", b.CollName, b.SyncId)
			}
		}
		if ms.oplog != nil {
			// store all syncId that is greater than
			// from what we are resuming sync for each collection,
			// so, we know whether to ignore oplog related to that coll until it comes to collSyncId
			ms.collSyncId[b.CollName] = b.SyncId
		}
	}
	if ms.oplog == nil { // give up on resume, start from current state after cloning collections
		ms.oplog, err = ms.GetDbOpLog(ctx, ms.Sender, "")
	}
	return err
}

// processOpLog handles incoming oplog entries from oplog channel.
// It finds out which collection that oplog record belongs.
// If a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel.
// Then it redirects oplog entry to that channel.
// If SyncId for that oplog is equal or greater than syncId for the collection
func (ms *MongoSync) processOpLog() {
	// each collection has separate channel for receiving its events.
	// that channel is served by dedicated goroutine
	ms.collChan = make(map[string]chan<- bson.Raw, 128)
	defer func() {
		// close channels for oplog operations
		for _, ch := range ms.collChan {
			ch <- nil // flush them before closing
			close(ch)
		}
	}()
	ms.initIdle()
	// loop until context tells we are done
	for op := range ms.oplog {
		// find out the name of the collection
		// check if it is synced
		coll := getColl(op)
		delay, batch, rt := ms.collMatch(coll)
		if delay == -1 {
			continue // ignore sync for this collection
		}
		// find out the channel for collection
		if ch, ok := ms.collChan[coll]; ok {
			// if a channel is there - the sync is underway, no need for other checks
			ch <- op
			continue
		}
		if !rt {
			// check whether we reached syncId to start collection sync
			syncId := getSyncId(op)
			if startFrom, ok := ms.collSyncId[coll]; ok && syncId < startFrom {
				// ignore operation while current sync_id is less than collection's one
				continue
			}
			log.Debugf("start follow coll %s from %s", coll, syncId)
		}
		// now establish handling channel for that collection so the next op can be handled
		ms.idleMutex.Lock()
		ms.collChan[coll] = ms.getCollChan(coll, delay, batch, rt)
		ms.idleMutex.Unlock()
		ms.collChan[coll] <- op
	}
}
