package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"regexp"
	"strings"
	"sync"
	"yadex/config"
	"yadex/mdb"
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
	Coll       string
	RealTime   bool
	OpType     OpLogType
	SyncId     string
	Models     []mongo.WriteModel
	TotalBytes int
}

type Oplog = <-chan bson.Raw

type MongoSync struct {
	ctx               context.Context
	cancelFunc        context.CancelFunc
	Sender, Receiver  *mongo.Database
	senderClient      *mongo.Client
	receiverClient    *mongo.Client
	senderAvailable   chan bool         // this channel follows availability of the sender server
	receiverAvailable chan bool         // this channel follows availability of the receiver server
	bwRT, bwST        chan *BulkWriteOp // channels for RT and ST BulkWrites
	Config            *config.ExchangeConfig
	collSyncId        map[string]string
	routines          sync.WaitGroup // housekeeping of runSync. it is zero on exit
	CollSyncId        *mongo.Collection
	oplog             Oplog
	collMatch         CollMatch
	collChan          map[string]chan<- bson.Raw // any collection has it's dedicated channel with dedicated goroutine.
	// checkIdle facility, see checkIdle.go
	wgRT             sync.WaitGroup // this is used to prevent ST BulkWrites clash over RT ones
	oplogIdle        bool
	oplogMutex       sync.RWMutex
	collBuffers      map[string]int
	pendingBuffers   int // pending bytes in collection's buffers
	collBuffersMutex sync.RWMutex
	pendingBulkWrite int // total of bulkWrite buffers queued at bwRT and bwST channels
	bulkWriteMutex   sync.RWMutex
	// flushUpdates is for FLUSH signal when oplog idling
	flushUpdates chan struct{}
	// idle is triggered from flushUpdates when there is nothing left unsent to the server
	idle chan struct{}
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig) (*MongoSync, error) {
	ms := &MongoSync{Config: exchCfg}
	ms.Config = exchCfg
	log.Infof("Establishing exchange %s...", ms.Name())
	var err error
	ms.senderClient, ms.senderAvailable, err = mdb.ConnectMongo(ctx, ms.Config.SenderURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
	log.Infof("Sucessfully connected to sender's DB at %s", ms.Config.SenderURI)
	ms.Sender = ms.senderClient.Database(ms.Config.SenderDB)
	ms.receiverClient, ms.receiverAvailable, err = mdb.ConnectMongo(ctx, ms.Config.ReceiverURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for receiver: %w", err)
	}
	ms.Receiver = ms.receiverClient.Database(ms.Config.ReceiverDB)
	log.Infof("Sucessfully connected to receiver's DB at %s", ms.Config.ReceiverURI)
	ms.collMatch = GetCollMatch(ms.Config) //
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.CollSyncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	return ms, nil
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
			sAvail, rAvail := "", ""
			if !senderAvail {
				sAvail = " not"
			}
			if !receiverAvail {
				rAvail = " not"
			}
			log.Debugf("sender:%s available, receiver:%s available", sAvail, rAvail)
			if exAvail {
				ms.routines.Add(1)
				log.Infof("Connection available, start syncing of %s again...", ms.Name())
				go ms.runSync(exCtx)
			} else {
				log.Warnf("Exchange unavailable, stopping %s... ", ms.Name())
				exCancel()
				ms.routines.Wait()
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
	defer ms.routines.Done()
	ms.initSync(ctx)
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
	ms.processOpLog(ctx)
}

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}
