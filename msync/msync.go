package mongosync

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"
	"yadex/config"
)

type (
	// enum type to define what kind of operation is BulkWriteOp: (OpLogUnknown, OpLogUnordered, OpLogOrdered, OpLogDrop, OpLogCloneCollection)
	OpLogType = int
	// this is postponed operation of modification which is put into prioritized channel
	// it arrives as an input for goroutine inside getBulkWriteOp(), see switch there to find out how it is processed
	BulkWriteOp struct {
		Coll     string
		RealTime bool
		OpType   OpLogType
		SyncId   string
		Models   []mongo.WriteModel
	}
	Oplog = <-chan bson.Raw
)

// enum OpLogType
const (
	OpLogUnknown   OpLogType = iota // we don't know how to handle this operation
	OpLogUnordered                  // BulkWrite should have ordered:no option
	OpLogOrdered                    // don't use unordered option, default ordered is true
	OpLogDrop                       // indicates current operation is drop
)

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

type putBulkWriteOp = func(bwOp *BulkWriteOp) *sync.WaitGroup

// getBulkWriteOp returns func which is used to add bulkWrite operation to buffered channel
// this function puts BulkWrite operation only if this is RealTime op or channel is clean
// it launches goroutine which serves  operation from that channel and
// 1. sends bulkWrites to receiver mongodb
// 2. stores sync_id into bookmarkCollSyncId for the ST collection on successful sync operation
func (ms *MongoSync) getBulkWriteOp() putBulkWriteOp {
	// create buffered (3) BulkWrite channel
	bwChan := make(chan *BulkWriteOp, 3)
	ctx := ms.ctx
	// ChanBusy is used to implement privileged access to BulkWriteOp channel for RT vs ST ops
	// it counts how many ops were put into the channel
	// ST ops always wait before  channel is empty and only then it is being put into channel
	var ChanBusy sync.WaitGroup
	// this goroutine serves BulkWriteOp channel
	serveBWChan := func() {
		for bwOp := range bwChan {
			ChanBusy.Done()
			// check if context is not expired
			if ctx.Err() != nil {
				return
			}
			collAtReceiver := ms.Receiver.Collection(bwOp.Coll)
			if bwOp.OpType == OpLogDrop {
				log.Tracef("drop Receiver.%s", bwOp.Coll)
				if err := collAtReceiver.Drop(ctx); err != nil {
					log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
				}
			} else {
				ordered := bwOp.OpType == OpLogOrdered
				optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
				if len(bwOp.Models) > 0 {
					r, err := collAtReceiver.BulkWrite(ctx, bwOp.Models, optOrdered)
					if err != nil {
						// here we do not consider "DuplicateKey" as an error.
						// If the record with DuplicateKey is already on the receiver - it is fine
						if ordered || !mongo.IsDuplicateKeyError(err) {
							log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
						}
					} else {
						log.Tracef("BulkWrite %s:%+v", bwOp.Coll, r)
					}
				}
			}
			// update sync_id for the ST collection
			if !bwOp.RealTime && bwOp.SyncId != "" {
				upsert := true
				optUpsert := &options.ReplaceOptions{Upsert: &upsert}
				id := bson.E{Key: "_id", Value: bwOp.Coll}
				filter := bson.D{id}
				doc := bson.D{id, {Key: "sync_id", Value: bwOp.SyncId}}
				if r, err := ms.CollSyncId.ReplaceOne(ctx, filter, doc, optUpsert); err != nil {
					log.Warnf("failed to update sync_id for collAtReceiver %s: %s", bwOp.Coll, err)
				} else {
					log.Debugf("Coll %s %d sync_id %s", bwOp.Coll, r.UpsertedCount, bwOp.SyncId)
				}
			}
		}
	}
	// run few parallel serveBWChan() goroutines
	for i := 0; i < 2; i++ {
		go serveBWChan()
	}

	// this mutex needed to avoid two or more separate ST detects that channel is empty.
	// before waiting, it Locks and after ChanBusy.Add(1) it unlocks so
	// next ST will be waiting until channel is empty again
	var ChanBusyMutex sync.Mutex // need for exclusive wait when ST op invoked
	// returns putBWOp func which closes bwChan if bwOp is nil,
	// otherwise puts bwOp RT or ST operation
	// for ST operation makes sure bwChan is empty
	return func(bwOp *BulkWriteOp) *sync.WaitGroup {
		if bwOp == nil {
			close(bwChan)
			return &ChanBusy
		}
		if bwOp.RealTime {
			ChanBusy.Add(1)
		} else {
			ChanBusyMutex.Lock() // one ST at a time
			ChanBusy.Wait()      // wait until channel is clean
			ChanBusy.Add(1)
			ChanBusyMutex.Unlock()
		}
		bwChan <- bwOp
		return &ChanBusy
	}
}

// getCollChan returns channel of Oplog for the collection.
// it launches goroutine which pops operation from that channel and flushes BulkWriteOp using putBWOp func
func getCollChan(collName string, maxDelay, maxBatch int64, realtime bool, putBWOp putBulkWriteOp) chan<- bson.Raw {
	in := make(chan bson.Raw)
	// buffer for BulkWrite
	var models []mongo.WriteModel
	var lastOpType OpLogType
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	// flush bulkWrite models or drop operation into BulkWrite channel
	flush := func() {
		ftCancel()
		if len(models) == 0 && lastOpType != OpLogDrop { // no op
			return
		}
		syncId := getSyncId(lastOp)
		bwOp := &BulkWriteOp{Coll: collName, RealTime: realtime, OpType: lastOpType, SyncId: syncId}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
		}
		models = nil
		putBWOp(bwOp)
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	go func() { // oplog for collection
		defer ftCancel()
		for op := range in { // oplog
			if op == nil {
				// this is from timer (go <-time.After(maxDelay))
				// look below at case <-time.After(time.Millisecond * time.Duration(maxDelay)):
				flush()
				continue
			}
			opType, writeModel := getWriteModel(op)
			// decide whether we need to flush before we can continue
			switch opType {
			case OpLogOrdered, OpLogUnordered:
				if lastOpType != opType {
					flush()
				}
			case OpLogDrop:
				models = nil
				lastOp = op
				lastOpType = opType
				flush()
				continue
			case OpLogUnknown: // ignore op
				log.Warnf("Operation of unknown type left unhandled:%+v", op)
				continue
			}
			lastOpType = opType
			lastOp = op
			models = append(models, writeModel)
			if len(models) >= int(maxBatch) {
				flush()
			} else if len(models) == 1 {
				// we just put 1st item, set timer to flush after maxDelay
				// unless it is filled up to (max)maxBatch items
				ftCancel()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				go func() {
					select {
					case <-flushTimer.Done():
						// we have done flush before maxDelay, so no need to flush after timer expires
						return
					case <-time.After(time.Millisecond * time.Duration(maxDelay)):
						ftCancel()
						in <- nil // flush() without lock
					}
				}()
			}
		}
	}()
	return in
}

// fetchCollSyncId fetches sorted by sync_id list of docs (collName, sync_id)
func fetchCollSyncId(ctx context.Context, bookmarkCollSyncId *mongo.Collection) (documents []bson.M, err error) {
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
func resumeOplog(ctx context.Context, sender *mongo.Database, bookmarkCollSyncId *mongo.Collection) (oplog Oplog, collSyncId map[string]string, err error) {
	csBookmarks, err := fetchCollSyncId(ctx, bookmarkCollSyncId)
	if ctx.Err() != nil {
		return // gracefully handle sync.Stop
	}
	if err != nil {
		log.Warn(err)
	}
	collSyncId = make(map[string]string, len(csBookmarks))
	for _, b := range csBookmarks {
		v, ok := b["sync_id"]
		if !ok {
			continue
		}
		syncId := v.(string)
		if oplog == nil {
			oplog, err = GetDbOpLog(ctx, sender, syncId)
			if oplog != nil {
				coll := b["_id"].(string)
				log.Infof("sync was resumed from (%s) %s", coll, syncId)
			}
		}
		if oplog != nil {
			// store all syncId that is greater than
			// from what we are resuming sync for each collection,
			// so, we know whether to ignore oplog related to that coll until it comes to collSyncId
			coll := b["_id"].(string)
			collSyncId[coll] = syncId
		}
	}
	if ctx.Err() != nil {
		return // gracefully handle sync.Stop
	}
	if oplog == nil { // give up on resume, start from current state after cloning collections
		oplog, err = GetDbOpLog(ctx, sender, "")
	}
	if ctx.Err() != nil {
		return // gracefully handle sync.Stop
	}
	return oplog, collSyncId, err
}

// getOpLog gets oplog and decodes it to bson.M. If it fails it returns nil
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
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplog before cloning collections
	return sender.Watch(ctx, pipeline, opts)
}

func GetDbOpLog(ctx context.Context, db *mongo.Database, syncId string) (<-chan bson.Raw, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw)
	go func() {
		defer close(ch)
		for {
			for changeStream.Next(ctx) {
				ch <- changeStream.Current
			}
			err = changeStream.Err()
			if errors.Is(err, context.Canceled) {
				log.Info("Process oplog gracefully shutdown")
				return
			} else if err == nil {
				for {
					log.Infof("Waiting for oplog to resume")
					time.Sleep(time.Second * 5)
					changeStream, err = getChangeStream(ctx, db, "")
					if err == nil {
						break
					}
					log.Errorf("Can't get oplog: %s", err)
				}
			} else {
				log.Errorf("exiting oplog processing: can't get next oplog entry %s", err)
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
	if _, err := coll.InsertOne(ctx, bson.M{"count": 1}); err != nil {
		return "", err
	}
	op, err := getOpLog(ctx, changeStreamLastSync)
	if op == nil {
		return "", err
	}
	return op["_id"].(bson.M)["_data"].(string), nil
}

type MongoSync struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	//senderClient, receiverClient *mongo.Client
	ExchangeAvailable chan bool
	Sender, Receiver  *mongo.Database
	Config            *config.ExchangeConfig
	CollSyncId        *mongo.Collection
}

// ConnectMongo establishes monitored connection to uri database server.
// connections state is broadcast by available channel.
// unless channel returned true there is no possibility to work with the connection
func ConnectMongo(ctx context.Context, uri string) (client *mongo.Client, available chan bool, err error) {
	available = make(chan bool, 1)
	svrMonitor := &event.ServerMonitor{
		TopologyDescriptionChanged: func(changedEvent *event.TopologyDescriptionChangedEvent) {
			servers := changedEvent.NewDescription.Servers
			avail := false
			for _, server := range servers {
				if server.AverageRTTSet {
					avail = true
				}
			}
			//log.Infof("db %s avail %v", uri, avail)
			available <- avail
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(svrMonitor)
	client, err = mongo.Connect(ctx, clientOpts)

	// wait until available
	avail := false
	for !avail {
		select {
		case <-ctx.Done():
			avail = true
		case avail = <-available:
		}
	}

	return client, available, ctx.Err()
}

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig, wg *sync.WaitGroup) (*MongoSync, error) {
	r := &MongoSync{}
	r.Config = exchCfg
	log.Infof("Establishing exchange %s...", r.Name())
	senderClient, senderAvailable, err := ConnectMongo(ctx, r.Config.SenderURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
	log.Infof("Sucessfully connected to sender's DB at %s", r.Config.SenderURI)
	// get name of bookmarkColSyncid on sender
	bookmarkColSyncIdName := strings.Join([]string{r.Config.SenderURI, r.Config.SenderDB, r.Config.ReceiverURI, r.Config.ReceiverDB}, "-")
	re := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = re.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	r.CollSyncId = senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	r.Sender = senderClient.Database(r.Config.SenderDB)
	receiverClient, receiverAvailable, err := ConnectMongo(ctx, r.Config.ReceiverURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for receiver: %w", err)
	}
	r.Receiver = receiverClient.Database(r.Config.ReceiverDB)
	log.Infof("Sucessfully connected to sender's DB at %s", r.Config.SenderURI)
	r.ExchangeAvailable = make(chan bool, 1)
	r.ExchangeAvailable <- true
	wg.Add(1)
	go func() {
		defer wg.Done()
		senderAvail := true
		receiverAvail := true
		exAvail := true
		for {
			if exAvail {
				r.Start(ctx)
			} else {
				r.Stop()
			}
			select {
			case <-ctx.Done():
				_ = senderClient.Disconnect(ctx)
				_ = receiverClient.Disconnect(ctx)
				close(r.ExchangeAvailable)
				return
			case senderAvail = <-senderAvailable:
			case receiverAvail = <-receiverAvailable:
			}
			if exAvail != senderAvail && receiverAvail {
				log.Infof("senderAvail:%v, ReceiverAvail:%v", senderAvail, receiverAvail)
				exAvail = senderAvail && receiverAvail
				r.ExchangeAvailable <- exAvail
			}
		}
	}()
	return r, nil
}

// Start launches synchronization between sender and receiver based on SyncConfig configuration.
// Use cancel (ctx) func to stop synchronization
// It is used after Stop in a case of Exchange is not available (either sender or receiver)
// So it creates all channels and trying to resume from the safe point
func (ms *MongoSync) Start(ctx context.Context) {
	ms.ctx, ms.cancelFunc = context.WithCancel(ctx)
	ctx = ms.ctx
	// create func to put BulkWriteOp into channel. It deals appropriate both with RT and ST
	opBW := ms.getBulkWriteOp()
	c := ms.Config
	collMatch := GetCollMatch(c) //
	oplog, collSyncId, err := resumeOplog(ctx, ms.Sender, ms.CollSyncId)
	if ctx.Err() != nil {
		return // gracefully handle sync.Stop
	}
	if err != nil {
		log.Errorf("Can't resume oplog: %s", err)
		return
	}
	// clone collections which we don't have bookmarks for restoring syncing using oplog
	if err := SyncCollections(ctx, collMatch, collSyncId, ms.Sender, ms.Receiver, opBW); err != nil {
		log.Errorf("failed to copy collections: %s, waiting for another chance", err)
		return
	}
	// start handling oplog for syncing sender and receiver collections
	processOpLog(oplog, collMatch, collSyncId, opBW)
	return
}
func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}
func (ms *MongoSync) Stop() {
	log.Infof("Stopping exchange %s", ms.Name())
	ms.cancelFunc()
}

// processOpLog handles incoming oplog entries from oplog channel
// it finds out which collection that oplog record belongs
// if a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel
// then it redirects oplog entry to that channel
// if SyncId for that oplog is equal or greater than syncId for the collection
func processOpLog(oplog Oplog, cm CollMatch, collSyncStartFrom map[string]string, bwOp putBulkWriteOp) {
	collChan := make(map[string]chan<- bson.Raw, 256)
	defer func() {
		// close channels for oplog operations
		for _, ch := range collChan {
			ch <- nil // flush them before closing
			close(ch)
		}
	}()
	// loop until context tells we are done
	for op := range oplog {
		// find out the name of the collection
		// check if it is synced
		coll := getColl(op)
		delay, batch, rt := cm(coll)
		if delay == -1 {
			continue // ignore sync for this collection
		}
		// find out the channel for collection
		if ch, ok := collChan[coll]; ok {
			// if a channel is there - the sync is underway, no need for other checks
			ch <- op
			continue
		}
		if !rt {
			// check whether we reached syncId to start collection sync
			syncId := getSyncId(op)
			if startFrom, ok := collSyncStartFrom[coll]; ok && syncId < startFrom {
				// ignore operation while current sync_id is less than collection's one
				continue
			}
			log.Debugf("start follow coll %s from %s", coll, syncId)
		}
		// now establish handling channel for that collection so the next op can be handled
		collChan[coll] = getCollChan(coll, delay, batch, rt, bwOp)
		collChan[coll] <- op
	}
}
