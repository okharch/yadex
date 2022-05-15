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

// getOplog creates channel providing oplog ops. It triggers(
//true) idle channel when there is not pending ops in the oplog and it isRToplog
func (ms *MongoSync) getOplog(ctx context.Context, db *mongo.Database, syncId string) (Oplog, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw, 1) // need buffered for unit tests
	ms.routines.Add(1)           // getOplog
	// provide oplogST entries to the channel, closes channel upon exit
	go func() {
		defer func() {
			ms.routines.Done()
			close(ch)
		}()
		for {
			next := changeStream.TryNext(ctx)
			if !next && ctx.Err() == nil {
				log.Infof("getOplog: no pending oplog. pending buf %v pending bulkWrite %d, exchange: %s", ms.getCollUpdated(),
					ms.getPendingBulkWrite(), ms.Name())
				// check "clean" condition only when tail of oplogST reached
				if ch == ms.oplogST || ms.oplogST == nil {
					log.Tracef("oplogST idling, check dirt")
					ms.dirty <- false // ST oplog tells it will be idling
				}
				next = changeStream.Next(ctx)
				log.Tracef("oplog idle finished: %s", getOpName(changeStream.Current))
			}
			if next {
				ch <- changeStream.Current
				continue
			}
			err = ctx.Err()
			if errors.Is(err, context.Canceled) {
				log.Info("Process oplog gracefully shutdown")
				return
			} else if err != nil {
				log.Errorf("shutdown oplog : can't get next oplogST entry %s", err)
				return
			}
			err := changeStream.Err()
			if err != nil {
				log.Errorf("failed to get oplog entry: %s", err)
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
	collSyncId := make(map[string]string, len(csBookmarks))
	var oplog Oplog
	if err != nil {
		return fmt.Errorf("failed to get sync bookmark, perhaps oplog is off: %w", err)
	}
	for _, b := range csBookmarks {
		if oplog == nil {
			oplog, err = ms.getOplog(ctx, ms.Sender, b.SyncId)
			if oplog != nil {
				log.Debugf("sync was resumed from (%s) %s", b.CollName, b.SyncId)
			}
		}
		if oplog != nil {
			// store all syncId that is greater or equal than
			// from what we are resuming sync for each collection,
			// so, we know whether to ignore oplogST related to that coll until it comes to collSyncId
			collSyncId[b.CollName] = b.SyncId
		}
	}
	if oplog == nil { // give up on resume, start from current state after cloning collections
		oplog, err = ms.getOplog(ctx, ms.Sender, "")
	}
	ms.oplogST = oplog
	ms.routines.Add(1)
	go func() {
		defer ms.routines.Done()
		ms.SyncCollections(ctx, collSyncId)
		ms.runSToplog(ctx, collSyncId)
	}()
	return err
}

// runSToplog handles incoming oplogST entries from oplogST channel.
// it calls getCollChan to redirect oplog to channel for an ST collection.
// If SyncId for the collection is greater than current syncId - it skips op
func (ms *MongoSync) runSToplog(ctx context.Context, collSyncId map[string]string) {
	defer ms.routines.Done() // runSToplog
	// loop until context tells we are done
	for op := range ms.oplogST {
		collName := getCollName(op)
		// check if it is subject to sync
		config, rt := ms.collMatch(collName)
		if rt || config == nil {
			continue // ignore unknown and RT collection
		}
		// check whether we reached syncId to start collection sync
		startFrom, ok := collSyncId[collName]
		if ok {
			syncId := getSyncId(op)
			if syncId < startFrom {
				continue
			}
			log.Infof("follow oplog for coll %s from %s, exchange %s", collName, syncId, ms.Name())
			delete(collSyncId, collName)
		}
		// now redirect handling of op to the channel for that collection
		ch := ms.getCollChan(ctx, collName, config, rt)
		ch <- op
	}
	log.Debug("runSToplog gracefully shutdown on cancelled context")
}

// initRToplog just starts to follow changes from the current tail of the oplog
func (ms *MongoSync) initRToplog(ctx context.Context) (err error) {
	ms.oplogRT, err = ms.getOplog(ctx, ms.Sender, "")
	return err
}

// runRToplog handles incoming oplogRT entries from oplogRT channel.
// It finds out which collection that oplogRT op belongs.
// If a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel.
// Then it redirects oplogRT op to that channel.
func (ms *MongoSync) runRToplog(ctx context.Context) {
	defer ms.routines.Done() // runRToplog
	for op := range ms.oplogRT {
		collName := getCollName(op)
		// check if it is synced
		config, rt := ms.collMatch(collName)
		if rt {
			// now get handling channel for that collection and direct op to that channel
			ch := ms.getCollChan(ctx, collName, config, rt)
			ch <- op
		}
	}
	log.Debug("runRToplog gracefully shutdown on cancelled context")
}
