package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"strings"
	"sync"
	"yadex/config"
)

// OpLogType enums what kind of operation is BulkWriteOp: (OpLogUnknown, OpLogUnordered, OpLogOrdered, OpLogDrop)
type OpLogType = int

// enum OpLogType
const (
	OpLogUnknown   OpLogType = iota // we don't know how to handle this operation
	OpLogUnordered                  // BulkWrite should have ordered:no option
	OpLogOrdered                    // don't use unordered option, default ordered is true
	OpLogDrop                       // indicates current operation is drop
)

// BulkWriteOp describes postponed (bulkWrite) operation of modification which is sent to the receiver
type BulkWriteOp struct {
	Coll     string
	RealTime bool
	OpType   OpLogType
	SyncId   string
	Models   []mongo.WriteModel
}

type Oplog = <-chan bson.Raw

type collCount struct {
	coll  string
	count int
}

type MongoSync struct {
	ctx               context.Context
	cancelFunc        context.CancelFunc
	Sender, Receiver  *mongo.Database
	senderClient      *mongo.Client
	receiverClient    *mongo.Client
	senderAvailable   chan bool
	receiverAvailable chan bool
	bwChan            chan *BulkWriteOp
	collCountChan     chan collCount
	Config            *config.ExchangeConfig
	collSyncId        map[string]string
	bwChanBusy        sync.Mutex // need for exclusive wait when ST op invoked
	ChanBusy          sync.WaitGroup
	Sync              sync.WaitGroup // housekeeping of runSync. it is zero on exit
	CollSyncId        *mongo.Collection
	oplog             Oplog
	collMatch         CollMatch
	collChan          map[string]chan<- bson.Raw
	idle              func()
	idleChan          chan struct{}
	// housekeeping of active exchanges. As soon as there is some problem with connection, etc. it becomes zero
	waitExchanges *sync.WaitGroup
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// InitBulkWriteChan creates bwChan chan *BulkWriteOp field which is used to add bulkWrite operation to buffered channel
// this function puts BulkWrite operation only if this is RealTime op or channel is clean
// it launches goroutine which serves  operation from that channel and
// 1. sends bulkWrites to receiver mongodb
// 2. stores sync_id into CollSyncId for the ST collection on successful sync operation
func (ms *MongoSync) InitBulkWriteChan(ctx context.Context) {
	// get name of bookmarkColSyncid on sender
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.CollSyncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	// create buffered (3) BulkWrite channel
	ms.bwChan = make(chan *BulkWriteOp, 3)
	// ChanBusy is used to implement privileged access to BulkWriteOp channel for RT vs ST ops
	// it counts how many ops were put into the channel
	// ST ops always wait before  channel is empty and only then it is being put into channel
	// this goroutine serves BulkWriteOp channel
	serveBWChan := func() {
		for bwOp := range ms.bwChan {
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
					log.Debugf("Coll found(updated) %d(%d) %s.sync_id %s", r.MatchedCount, r.UpsertedCount+r.ModifiedCount, bwOp.Coll,
						bwOp.SyncId)
				}
			}
			ms.ChanBusy.Done()
		}
	}
	// runSync few parallel serveBWChan() goroutines
	for i := 0; i < 2; i++ {
		go serveBWChan()
	}

}

// putBwOp puts BulkWriteOp to BulkWriteChan.
// special value nil is used to make sure bwChan is empty
// It respects RT vs ST priority.
// It puts RT bwOp without delay
// It will wait until channel is empty and only then will put ST bwOp
func (ms *MongoSync) putBwOp(bwOp *BulkWriteOp) {
	//log.Debugf("putBwOp: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
	if bwOp != nil && bwOp.RealTime {
		ms.ChanBusy.Add(1)
	} else {
		// this mutex needed to avoid two or more separate ST finding that channel is empty.
		// before waiting, it Locks and after ChanBusy.Add(1) it unlocks so
		// next ST will be waiting until channel is empty again
		ms.bwChanBusy.Lock() // one ST at a time
		//log.Debugf("putBwOp.Wait: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
		ms.ChanBusy.Wait() // wait until channel is clean
		if bwOp != nil {
			ms.ChanBusy.Add(1)
		}
		ms.bwChanBusy.Unlock()
	}
	//log.Debugf("putBwOp->channel: %s %v %d", bwOp.Coll,bwOp.OpType, len(bwOp.Models))
	if bwOp != nil {
		ms.bwChan <- bwOp
	}
	return
}

func (ms *MongoSync) WaitFlushed() {
	<-ms.idleChan
	ms.putBwOp(nil)
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
		v, ok := b["sync_id"]
		if !ok {
			continue
		}
		syncId := v.(string)
		if ms.oplog == nil {
			ms.oplog, err = GetDbOpLog(ctx, ms.Sender, syncId)
			if ms.oplog != nil {
				coll := b["_id"].(string)
				log.Infof("sync was resumed from (%s) %s", coll, syncId)
			}
		}
		if ms.oplog != nil {
			// store all syncId that is greater than
			// from what we are resuming sync for each collection,
			// so, we know whether to ignore oplog related to that coll until it comes to collSyncId
			coll := b["_id"].(string)
			ms.collSyncId[coll] = syncId
		}
	}
	if ms.oplog == nil { // give up on resume, start from current state after cloning collections
		ms.oplog, err = GetDbOpLog(ctx, ms.Sender, "")
	}
	return err
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

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig, waitExchanges *sync.WaitGroup) (*MongoSync, error) {
	r := &MongoSync{Config: exchCfg, waitExchanges: waitExchanges}
	r.Config = exchCfg
	log.Infof("Establishing exchange %s...", r.Name())
	var err error
	r.senderClient, r.senderAvailable, err = ConnectMongo(ctx, r.Config.SenderURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
	log.Infof("Sucessfully connected to sender's DB at %s", r.Config.SenderURI)
	r.Sender = r.senderClient.Database(r.Config.SenderDB)
	r.receiverClient, r.receiverAvailable, err = ConnectMongo(ctx, r.Config.ReceiverURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for receiver: %w", err)
	}
	r.Receiver = r.receiverClient.Database(r.Config.ReceiverDB)
	log.Infof("Sucessfully connected to receiver's DB at %s", r.Config.ReceiverURI)
	r.collMatch = GetCollMatch(r.Config) //
	return r, nil
}

// Run launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used after Stop in a case of Exchange is not available (either sender or receiver)
// So it creates all channels and trying to resume from the safe point
func (ms *MongoSync) Run(ctx context.Context) {
	senderAvail := true
	receiverAvail := true
	exAvail := true
	oldAvail := false
	exCtx, exCancel := context.WithCancel(ctx)
	// this loop watches over parent context and exchange's connections available
	// if they are not, it cancels exCtx so derived channels could be closed and synchronization stopped
	for {
		if oldAvail != exAvail {
			log.Infof("senderAvail:%v, ReceiverAvail:%v", senderAvail, receiverAvail)
			if exAvail {
				ms.waitExchanges.Add(1)
				ms.Sync.Add(1)
				go ms.runSync(exCtx)
			} else {
				log.Infof("Stopping exchange %s", ms.Name())
				exCancel()
				ms.Sync.Wait()
				ms.waitExchanges.Done()
				exCtx, exCancel = context.WithCancel(ctx)
			}
			oldAvail = exAvail
		}
		select {
		case <-ctx.Done():
			ctx := context.TODO()
			_ = ms.senderClient.Disconnect(ctx)
			_ = ms.receiverClient.Disconnect(ctx)
			exCancel()
			return
		case senderAvail = <-ms.senderAvailable:
		case receiverAvail = <-ms.receiverAvailable:
		}
		exAvail = senderAvail && receiverAvail
	}
}

// runSync launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used in a case of Exchange is not available (either sender or receiver connection fails)
// it creates all the channels and trying to resume from the safe point
func (ms *MongoSync) runSync(ctx context.Context) {
	// create func to put BulkWriteOp into channel. It deals appropriate both with RT and ST
	defer ms.waitExchanges.Done()
	defer ms.Sync.Done()
	ms.InitBulkWriteChan(ctx)
	err := ms.resumeOplog(ctx)
	if ctx.Err() != nil {
		return // gracefully handle sync.Stop
	}
	if err != nil {
		log.Errorf("Can't resume oplog: %s", err)
		return
	}
	// clone collections which we don't have bookmarks for restoring from oplog
	if err := ms.SyncCollections(ctx); err != nil {
		log.Errorf("failed to copy collections: %s, waiting for another chance", err)
		return
	}
	// start handling oplog for syncing sender and receiver collections
	ms.processOpLog()
}

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}

// processOpLog handles incoming oplog entries from oplog channel
// it finds out which collection that oplog record belongs
// if a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel
// then it redirects oplog entry to that channel
// if SyncId for that oplog is equal or greater than syncId for the collection
func (ms *MongoSync) processOpLog() {
	ms.collChan = make(map[string]chan<- bson.Raw, 256)
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
		if op == nil {
			ms.idle()
			continue
		}
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
		ms.collChan[coll] = ms.getCollChan(coll, delay, batch, rt)
		ms.collChan[coll] <- op
	}
}

func (ms *MongoSync) initIdle() {
	ms.collCountChan = make(chan collCount, 1)
	collCount := make(map[string]int)
	var collMax string
	var collMaxCount int
	refreshMax := func() {
		// find new collMaxCount
		collMaxCount = 0
		collMax = ""
		for coll, count := range collCount {
			if count > collMaxCount {
				collMax = coll
				collMaxCount = count
			}
		}
	}
	// handle collCount channel
	go func() {
		for cc := range ms.collCountChan {
			if cc.count == 0 {
				delete(collCount, cc.coll)
				if cc.coll == collMax {
					refreshMax()
				}
				continue
			}
			if cc.count > collMaxCount {
				collMax = cc.coll
				collMaxCount = cc.count
			}
			collCount[cc.coll] = cc.count
		}
	}()
	ms.idleChan = make(chan struct{}, 1)
	ms.idle = func() {
		// non-blocking read
		select {
		case <-ms.idleChan:
		default:
		}
		if collMaxCount > 0 {
			// this will refreshMax upon flushing
			ms.collChan[collMax] <- nil
		} else {
			ms.idleChan <- struct{}{}
		}
	}
}
