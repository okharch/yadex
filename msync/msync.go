package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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
	// housekeeping of active exchanges. As soon as there is some problem with connection, etc. it becomes zero
	waitExchanges *sync.WaitGroup
	// checkIdle facility, see checkIdle.go
	idleChan     chan struct{}
	collChan     map[string]chan<- bson.Raw
	collCount    map[string]int
	collMax      string
	collMaxCount int
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig, waitExchanges *sync.WaitGroup) (*MongoSync, error) {
	r := &MongoSync{Config: exchCfg, waitExchanges: waitExchanges}
	r.Config = exchCfg
	log.Infof("Establishing exchange %s...", r.Name())
	var err error
	r.senderClient, r.senderAvailable, err = mdb.ConnectMongo(ctx, r.Config.SenderURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
	log.Infof("Sucessfully connected to sender's DB at %s", r.Config.SenderURI)
	r.Sender = r.senderClient.Database(r.Config.SenderDB)
	r.receiverClient, r.receiverAvailable, err = mdb.ConnectMongo(ctx, r.Config.ReceiverURI)
	if err != nil {
		return nil, fmt.Errorf("failed to establish client for receiver: %w", err)
	}
	r.Receiver = r.receiverClient.Database(r.Config.ReceiverDB)
	log.Infof("Sucessfully connected to receiver's DB at %s", r.Config.ReceiverURI)
	r.collMatch = GetCollMatch(r.Config) //
	r.initIdle()
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
			sAvail, rAvail := "", ""
			if !senderAvail {
				sAvail = " not"
			}
			if !receiverAvail {
				rAvail = " not"
			}
			log.Infof("sender:%s available, receiver:%s available", sAvail, rAvail)
			if exAvail {
				ms.waitExchanges.Add(1)
				ms.Sync.Add(1)
				log.Infof("Connection available, start syncing of %s again...", ms.Name())
				go ms.runSync(exCtx)
			} else {
				log.Infof("Exchange unavailable, stopping %s... ", ms.Name())
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
