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
	"time"
)

type (
	// enum type to define what kind of operation is BulkWriteOp: (OpLogUnknown, OpLogUnordered, OpLogOrdered, OpLogDrop, OpLogCloneCollection)
	OpLogType = int
	// this is postponed operation of modification which is put into prioritized channel
	// it arrives as an input for goroutine inside GetBulkWriteOpChan(), see switch there to find out how it is processed
	BulkWriteOp struct {
		Coll   string
		OpType OpLogType
		SyncId string
		Models []mongo.WriteModel
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

// GetBulkWriteOpChan returns write-only channel for BulkWrite operations
// it launches goroutine which pops operation from that channel and
// 1. sends bulkWrites to receiver mongodb
// 2. stores sync_id into bookmarkCollSyncId for the collection on successful sync operation
func GetBulkWriteOpChan(ctx context.Context, receiver *mongo.Database, bookmarkCollSyncId *mongo.Collection) chan<- BulkWriteOp {
	ch := make(chan BulkWriteOp)
	go func() {
		for bwOp := range ch {
			coll := receiver.Collection(bwOp.Coll)
			if bwOp.OpType == OpLogDrop {
				log.Tracef("drop Receiver.%s", bwOp.Coll)
				if err := coll.Drop(ctx); err != nil {
					log.Warnf("failed to drop collection %s at the receiver:%s", bwOp.Coll, err)
				}
			} else {
				ordered := bwOp.OpType == OpLogOrdered
				optOrdered := &options.BulkWriteOptions{Ordered: &ordered}
				if len(bwOp.Models) > 0 {
					r, err := coll.BulkWrite(ctx, bwOp.Models, optOrdered)
					if err != nil {
						log.Errorf("Connection lost to %s: %s", receiver.Name(), err)
					}
					if err != nil {
						if ordered || !mongo.IsDuplicateKeyError(err) {
							log.Errorf("failed BulkWrite to receiver.%s: %s", bwOp.Coll, err)
						}
					} else {
						log.Tracef("BulkWrite %s:%+v", bwOp.Coll, r)
					}
				}
			}
			// update sync_id for the collection
			if bwOp.SyncId != "" {
				upsert := true
				optUpsert := &options.ReplaceOptions{Upsert: &upsert}
				id := bson.E{Key: "_id", Value: bwOp.Coll}
				filter := bson.D{id}
				doc := bson.D{id, {Key: "sync_id", Value: bwOp.SyncId}}
				if r, err := bookmarkCollSyncId.ReplaceOne(ctx, filter, doc, optUpsert); err != nil {
					log.Warnf("failed to update sync_id for coll %s: %s", bwOp.Coll, err)
				} else {
					log.Tracef("Coll %s %d sync_id %s", bwOp.Coll, r.UpsertedCount, bwOp.SyncId)
				}
			}
		}
	}()
	return ch
}

// getCollChan returns channel of Oplog for the coll collection.
// it launches goroutine which pops operation from that channel and flushes BulkWritesOps to bulkWriteChan channel
func getCollChan(coll string, maxBulkCount int, maxDelay time.Duration,
	bulkWriteChan chan BulkWriteOp) chan<- bson.Raw {
	in := make(chan bson.Raw)
	// buffer for BulkWrite
	var models []mongo.WriteModel
	var lastOpType OpLogType
	var lastOp bson.Raw
	flushTimer, ftCancel := context.WithCancel(context.Background())
	// flushes Models or drop operation into BulkWrite channel
	flush := func() {
		ftCancel()
		if lastOpType != OpLogDrop && len(models) == 0 {
			return
		}
		syncId := getSyncId(lastOp)
		bwOp := BulkWriteOp{Coll: coll, OpType: lastOpType, SyncId: syncId}
		if lastOpType != OpLogDrop {
			bwOp.Models = models
		}
		models = nil
		bulkWriteChan <- bwOp
	}
	// process channel of Oplog, collects similar operation to batches, flushing them to bulkWriteChan
	go func() { // oplog for collection
		defer ftCancel()
		for op := range in { // oplog
			if op == nil { // this is from timer (go <-time.After(maxDelay))
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
			if len(models) >= maxBulkCount {
				flush()
			} else if len(models) == 1 { // set timer to flush after maxDelay
				ftCancel()
				flushTimer, ftCancel = context.WithCancel(context.Background())
				go func() {
					select {
					case <-flushTimer.Done():
						// we have done flush before maxDelay, so no need to flush after timer expires
						return
					case <-time.After(maxDelay):
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
// It tries to resume from that id.
// it returns collSyncId map for all collections that has greater sync_id.
// if it fails to resume from any stored sync_id it starts from current oplog and returns empty collSyncId
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

// cloneCollection clones collection by sending bulk write ops into out channel
func cloneCollection(ctx context.Context, sender *mongo.Database, collName string, syncId string, maxBulkCount int, out chan<- BulkWriteOp) error {
	log.Infof("cloning collection %s...", collName)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	out <- BulkWriteOp{
		Coll:   collName,
		OpType: OpLogDrop,
	}
	models := make([]mongo.WriteModel, maxBulkCount)
	count := 0
	flush := func() {
		if ctx.Err() != nil {
			return
		}
		out <- BulkWriteOp{
			Coll:   collName,
			OpType: OpLogUnordered,
			Models: models[:count],
		}
		models = make([]mongo.WriteModel, maxBulkCount)
		count = 0
	}
	coll := sender.Collection(collName)
	cursor, err := coll.Find(ctx, allRecords)
	if err != nil {
		return fmt.Errorf("ErrCloneCollection: can't read from sender.%s : %w", collName, err)
	}
	for cursor.Next(ctx) {
		raw := make([]byte, len(cursor.Current))
		copy(raw, cursor.Current) // TODO: copy bson.Raw - find out if that works
		models[count] = &mongo.InsertOneModel{Document: bson.Raw(raw)}
		count++
		if count == maxBulkCount {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			flush()
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	flush()
	// write syncId
	if ctx.Err() != nil {
		return ctx.Err()
	}
	out <- BulkWriteOp{
		Coll:   collName,
		OpType: OpLogUnordered,
		SyncId: syncId,
	}
	return nil
}

type MongoSync struct {
	ctx                          context.Context
	cancelFunc                   context.CancelFunc
	senderClient, receiverClient *mongo.Client
	ExchangeAvailable            chan bool
	Sender, Receiver             *mongo.Database
	Config                       *SyncConfig
	CollSyncId                   *mongo.Collection
}

// ConnectMongo establishes monitored connection to uri database server.
// connections state is broadcast by available channel.
// unless channel returned true there is no possibility to work with the connection
func ConnectMongo(ctx context.Context, uri string) (client *mongo.Client, available chan bool, err error) {
	available = make(chan bool)
	svrMonitor := &event.ServerMonitor{
		TopologyDescriptionChanged: func(changedEvent *event.TopologyDescriptionChangedEvent) {
			servers := changedEvent.NewDescription.Servers
			avail := false
			for _, server := range servers {
				if server.AverageRTTSet {
					avail = true
				}
			}
			available <- avail
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(svrMonitor)
	//clientOpts.SetConnectTimeout(time.Second*5)
	client, err = mongo.Connect(ctx, clientOpts)
	return client, available, err
}

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

func NewMongoSync(ctx context.Context, config *SyncConfig) (*MongoSync, error) {
	var r MongoSync
	r.Config = config
	senderClient, senderAvailable, err := ConnectMongo(ctx, r.Config.SenderURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
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
	r.ExchangeAvailable = make(chan bool)
	go func() {
		var senderAvail, receiverAvail bool
		for {
			select {
			case <-ctx.Done():
				_ = senderClient.Disconnect(ctx)
				_ = receiverClient.Disconnect(ctx)
				close(r.ExchangeAvailable)
				return
			case senderAvail = <-senderAvailable:
			case receiverAvail = <-receiverAvailable:
			}
			r.ExchangeAvailable <- senderAvail && receiverAvail
		}
	}()
	r.ctx, r.cancelFunc = context.WithCancel(context.Background())
	return &r, nil
}

// Start launches syncronization between sender and receiver based on SyncConfig configuration.
// Use cancel (ctx) func to stop syncronization
func (ms *MongoSync) Start() {
	ctx := ms.ctx
	for delay := time.Duration(0); ctx.Err() == nil; delay = ms.Run() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (ms *MongoSync) Run() (delay time.Duration) {
	ctx := ms.ctx
	opBW := GetBulkWriteOpChan(ctx, ms.Receiver, ms.CollSyncId)
	c := ms.Config
	colPriorityFunc, countPriority := GetCollPriority(c)
	chBulkWriteOps := GetPrioritizedChan(ctx, countPriority, opBW)
	oplog, collSyncId, err := resumeOplog(ctx, ms.Sender, ms.CollSyncId)
	if ctx.Err() != nil {
		return 0 // gracefully handle sync.Stop
	}
	delay = time.Second * 10
	if err != nil {
		log.Errorf("Can't resume oplog: %s", err)
		return
	}
	// clone collections which we don't have bookmarks for restore syncing
	if err := cloneCollections(colPriorityFunc, collSyncId, ctx, ms.Sender, c, chBulkWriteOps); err != nil {
		log.Errorf("failed to copy collection %s:%err, waiting for another chance", err)
		return
	}
	// start handling oplog for syncing sender and receiver collections
	processOpLog(oplog, colPriorityFunc, collSyncId, chBulkWriteOps, c)
	return 0
}

func (ms *MongoSync) Stop() {
	ms.cancelFunc()
}

// processOpLog handles incoming oplog entries from oplog channel
// it finds out which collection that oplog record belongs
// if a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel
// then it redirects oplog entry to that channel if SyncId for that oplog is equal or greater then syncId from map collSyncStartFrom
func processOpLog(oplog Oplog, colPriorityFunc CollPriorityFunc, collSyncStartFrom map[string]string, chBulkWriteOps []chan BulkWriteOp,
	c *SyncConfig) {
	collChan := make(map[string]chan<- bson.Raw, 512)
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
		db, coll := getNS(op)
		priority, BatchDelay, BatchSize := colPriorityFunc(db, coll)
		if priority == -1 {
			continue // ignore sync for this collection
		}
		// if not init, get config (c) values
		if BatchSize == 0 {
			BatchSize = c.BatchSize
		}
		if BatchDelay.Milliseconds() < 1 {
			BatchDelay = c.BatchDelay
		}
		// find out the channel for collection
		if ch, ok := collChan[coll]; ok {
			// if a channel is there - the sync is underway, no need for other checks
			ch <- op
			continue
		}
		// check whether we reached syncId to start collection sync
		syncId := getSyncId(op)
		if startFrom, ok := collSyncStartFrom[coll]; ok && syncId < startFrom {
			// ignore operation while current sync_id is less than collection's one
			continue
		}
		// now establish handling channel for that collection so the next op can be handled
		log.Debugf("start follow coll %s from %s", coll, syncId)
		collChan[coll] = getCollChan(coll, BatchSize, BatchDelay, chBulkWriteOps[priority])
		collChan[coll] <- op
	}
}

// cloneCollections checks for all collections from database whether they should be synced at all using colPriorityFunc != -1 flag
// then it checks whether msync database exchange collSyncId has saved bookmark where sync of that collection by oplog can be restored
// if sync can't be restored it saves current syncId for sender database and then clones all the documents of the collection f
// rom sender to receiver using collSyncId channel
// so it ignores non-synced or "can be synced from syncId" collections
// it returns nil if it was able to clone all the other collections successfuly into chBulkWriteOps channels
func cloneCollections(colPriorityFunc CollPriorityFunc, collSyncStartFrom map[string]string, ctx context.Context, sender *mongo.Database, c *SyncConfig, chBulkWriteOps []chan BulkWriteOp) error {
	// here we clone those collections which does not have sync_id
	// get all collections from database and clone those without sync_id
	colls, err := sender.ListCollectionNames(ctx, allRecords)
	if err != nil {
		return err
	}
	for _, coll := range colls {
		priority, _, _ := colPriorityFunc(sender.Name(), coll)
		if priority == -1 {
			continue
		}
		if _, ok := collSyncStartFrom[coll]; !ok {
			lastSyncId, err := getLastSyncId(ctx, sender)
			if err != nil {
				return fmt.Errorf("Can't fetch SyncId to start replication after collection %s clone: %w", coll, err)
			}
			if err := cloneCollection(ctx, sender, coll, lastSyncId, c.BatchSize, chBulkWriteOps[priority]); err != nil {
				return fmt.Errorf("failed to clone collection %s to receiver: %w", coll, err)
			}
			collSyncStartFrom[coll] = lastSyncId
		}
	}
	return nil
}