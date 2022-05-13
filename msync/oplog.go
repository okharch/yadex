package mongosync

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

// getChangeStream tries to resume sync from last successfully committed syncId.
// That id is stored for each mongo collection each time after successful write oplogST into receiver database.
func getChangeStream(ctx context.Context, sender *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplogST before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}
func (ms *MongoSync) runIdle(ctx context.Context) {
	defer ms.routines.Done()
	var idleST, idleRT bool
	for {
		select {
		case <-ctx.Done():
			return
		case idleST = <-ms.idleST:
		case idleRT = <-ms.idleRT:
		}
		if idleST && idleRT {
			ms.checkIdle("ST & RT")
		} else {
			ClearSignal(ms.idle)
		}
	}
}

func (ms *MongoSync) GetDbOpLog(ctx context.Context, db *mongo.Database, syncId string, idle chan bool) (Oplog, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw, 128)
	ms.routines.Add(1)
	// provide oplogST entries to the channel, closes channel upon exit
	go func() {
		ms.routines.Done()
		defer close(ch)
		for {
			next := changeStream.TryNext(ctx)
			if !next && ctx.Err() == nil {
				log.Infof("GetDbOpLog: no pending oplog. pending buf %d pending bulkWrite %d", ms.getPendingBuffers(),
					ms.getPendingBulkWrite())
				idle <- true
				next = changeStream.Next(ctx)
			}
			if next {
				ch <- changeStream.Current
				continue
			}
			err = ctx.Err()
			if errors.Is(err, context.Canceled) {
				log.Info("Process oplogST gracefully shutdown")
				return
			} else if err != nil {
				log.Errorf("shutdown oplogST : can't get next oplogST entry %s", err)
				return
			}
			err := changeStream.Err()
			if err != nil {
				log.Errorf("failed to get oplogST entry: %s", err)
				continue
			}
		}
	}()
	return ch, nil
}

func GetRandomHex(l int) string {
	bName := make([]byte, l)
	rand.Read(bName)
	return hex.EncodeToString(bName)
}

// initLastSyncId retrieves where oplogRT is now.
// It inserts some record into random collection which it then drops
// it returns syncIid for insert operation.
func (ms *MongoSync) initLastSyncId(ctx context.Context) error {
	coll := ms.Sender.Collection("rnd" + GetRandomHex(8))
	// make some update to generate oplogRT
	if _, err := coll.InsertOne(ctx, bson.D{}); err != nil {
		return err
	}
	op := <-ms.oplogRT
	ms.setLastSyncId(getSyncId(op))
	return coll.Drop(ctx)
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

func (ms *MongoSync) setLastSyncId(syncId string) {
	ms.Lock()
	ms.lastSyncId = syncId
	ms.Unlock()
}

func (ms *MongoSync) getLastSyncId() string {
	ms.RLock()
	defer ms.RUnlock()
	return ms.lastSyncId
}

// initSTOplog finds out minimal sync_id from collMSync collection.
// which it can successfully resume oplogST watch.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplogST
// and returns empty collSyncId
func (ms *MongoSync) initSTOplog(ctx context.Context) error {
	csBookmarks, err := fetchCollSyncId(ctx, ms.syncId)
	if ctx.Err() != nil {
		return ctx.Err() // gracefully handle sync.Stop
	}
	if err != nil {
		log.Warn(err)
	}
	ms.collSyncId = make(map[string]string, len(csBookmarks))
	var oplog Oplog
	if err != nil {
		return fmt.Errorf("failed to get sync bookmark, perhaps oplog is off: %w", err)
	}
	for _, b := range csBookmarks {
		if oplog == nil {
			oplog, err = ms.GetDbOpLog(ctx, ms.Sender, b.SyncId, ms.idleST)
			if oplog != nil {
				log.Debugf("sync was resumed from (%s) %s", b.CollName, b.SyncId)
			}
		}
		if oplog != nil {
			// store all syncId that is greater or equal than
			// from what we are resuming sync for each collection,
			// so, we know whether to ignore oplogST related to that coll until it comes to collSyncId
			ms.collSyncId[b.CollName] = b.SyncId
		}
	}
	if oplog == nil { // give up on resume, start from current state after cloning collections
		oplog, err = ms.GetDbOpLog(ctx, ms.Sender, "", ms.idleST)
	}
	ms.oplogST = oplog
	return err
}

// runSToplog handles incoming oplogST entries from oplogST channel.
// It finds out which collection that oplogST record belongs.
// If a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel.
// Then it redirects oplogST entry to that channel.
// If SyncId for that oplogST is equal or greater than syncId for the collection
func (ms *MongoSync) runSToplog(ctx context.Context) {
	defer ms.routines.Done()
	// loop until context tells we are done
	for op := range ms.oplogST {
		if ctx.Err() != nil {
			return
		}
		collName := getCollName(op)
		// check if it is synced
		config, rt := ms.collMatch(collName)
		if config == nil || rt {
			continue // ignore unknown and RT collection
		}
		// check whether we reached syncId to start collection sync
		syncId := getSyncId(op)
		if startFrom, ok := ms.collSyncId[collName]; ok && syncId < startFrom {
			// ignore operation while current sync_id is less than collection's one
			continue
		}
		log.Debugf("start follow coll %s from %s", collName, ms.lastSyncId)
		// now establish handling channel for that collection so the next op can be handled
		// now get handling channel for that collection and direct op to that channel
		ms.idleST <- false
		ch := ms.getCollChan(ctx, collName, config, rt)
		ch <- op
	}
}

// initRToplog just starts to follow changes from the current tail of the oplog
func (ms *MongoSync) initRToplog(ctx context.Context) (err error) {
	ms.oplogRT, err = ms.GetDbOpLog(ctx, ms.Sender, "", ms.idleRT)
	return err
}

// runRToplog handles incoming oplogRT entries from oplogRT channel.
// It finds out which collection that oplogRT op belongs.
// If a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel.
// Then it redirects oplogRT op to that channel.
func (ms *MongoSync) runRToplog(ctx context.Context) {
	for op := range ms.oplogRT {
		// update lastSyncId
		if ctx.Err() != nil {
			return
		}
		ms.setLastSyncId(getSyncId(op))
		collName := getCollName(op)
		// check if it is synced
		config, rt := ms.collMatch(collName)
		if !rt {
			continue
		}
		// now get handling channel for that collection and direct op to that channel
		ms.idleRT <- false
		ch := ms.getCollChan(ctx, collName, config, rt)
		ch <- op
	}
}
