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

type Oplog chan bson.Raw

const bwLogSize = 256

type MongoSync struct {
	sync.RWMutex
	ctx                      context.Context
	cancelFunc               context.CancelFunc
	Sender, Receiver         *mongo.Database
	senderClient             *mongo.Client
	receiverClient           *mongo.Client
	senderAvailable          chan bool         // this channel follows availability of the sender server
	receiverAvailable        chan bool         // this channel follows availability of the receiver server
	bulkWriteRT, bulkWriteST chan *BulkWriteOp // channels for RT and ST BulkWrites
	Config                   *config.ExchangeConfig
	lastSyncId               string
	collSyncId               map[string]string // which Id start/started collection syncing from
	routines                 sync.WaitGroup    // housekeeping of runSync. it is zero on exit
	// syncId collection to store sync progress bookmark for ST collections.
	// When it starts sync session for an ST collection again it tries to resume syncing from that bookmark.
	syncId *mongo.Collection
	// need both idleST and idleRT channels to find out complete idle situation
	// as we can idle in RT but not in ST and vice versa
	idleST    chan bool                  // declare the idleST state for oplogST
	idleRT    chan bool                  // declare the idleRT state for oplogRT
	oplogST   Oplog                      // follow ST changes
	oplogRT   Oplog                      // follow RT changes
	collMatch CollMatch                  // config, realtime nil and false if not found
	collChan  map[string]chan<- bson.Raw // any collection has it's dedicated channel with dedicated goroutine.
	// checkIdle facility, see checkIdle.go
	wgRT             sync.WaitGroup // this is used to prevent ST BulkWrites clash over RT line
	collBuffers      map[string]int // collName=>collBytes stores how many bytes pending in a collection buffer
	pendingBuffers   int            // pending bytes in collection's buffers
	collBuffersMutex sync.RWMutex
	pendingBulkWrite int // total of bulkWrite buffers queued at bulkWriteRT and bulkWriteST channels
	totalBulkWrite   int // this counts total bytes flushed to the receiver
	bulkWriteMutex   sync.RWMutex
	bwLog            [bwLogSize]BWLog
	bwLogIndex       int
	// flush is for FLUSH signal when oplogST idling
	flush chan struct{}
	// idle is triggered from BulkWrite when there is nothing left unsent to the server
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
	ms.collMatch = GetCollMatch(ms.Config) // config dependent, init by NewMongoSync
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.syncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	return ms, nil
}

// Run launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used after Stop in a case of Exchange is not available (either sender or receiver)
// So it creates all channels and trying to resume from the safe point
func (ms *MongoSync) Run(ctx context.Context) {
	senderAvail := false
	receiverAvail := false
	oldAvail := false
	exCtx, exCancel := context.WithCancel(ctx)
	// this loop watches over parent context and exchange's connections available
	// if they are not, it cancels exCtx so derived channels could be closed and synchronization stopped
	for {
		select {
		case <-ctx.Done():
			ctx := context.TODO()
			_ = ms.senderClient.Disconnect(ctx)
			_ = ms.receiverClient.Disconnect(ctx)
			exCancel()
			return
		case senderAvail = <-ms.senderAvailable:
			if senderAvail {
				log.Infof("sender %s is available", ms.Config.SenderURI)
			} else {
				log.Warnf("sender %s is not available", ms.Config.SenderURI)
			}
		case receiverAvail = <-ms.receiverAvailable:
			if receiverAvail {
				log.Infof("receiver %s is available", ms.Config.ReceiverURI)
			} else {
				log.Warnf("receiver %s is not available", ms.Config.ReceiverURI)
			}
		}
		exAvail := senderAvail && receiverAvail
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
				log.Infof("running exchange %s...", ms.Name())
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
	}
}

// runSync launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used in a case of Exchange is not available (either sender or receiver connection fails)
// it creates all the channels and trying to resume from the safe point
func (ms *MongoSync) runSync(ctx context.Context) {
	// create func to put BulkWriteOp into channel. It deals appropriate both with RT and ST
	if err := ms.initSync(ctx); err != nil {
		log.Fatalf("init failed: %s", err)
		return
	}
	ms.routines.Add(9)
	// close channels on expired context
	go ms.closeOnCtxDone(ctx)
	// show avg speed of BulkWrite ops to the log
	go ms.showSpeed(ctx) // optional, comment out if not needed
	// runSync few parallel serveBWChan() goroutines
	go ms.runRTBulkWrite(ctx)
	// 1 ST
	go ms.runSTBulkWrite(ctx)
	// flush server
	go ms.runFlush(ctx)
	// start handling oplogST for syncing sender and receiver collections
	go ms.runRToplog(ctx)
	go ms.runSToplog(ctx)
	go ms.runIdle(ctx)
	go ms.SyncCollections(ctx)
	ms.routines.Wait()
}

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}

func (ms *MongoSync) initChannels() {
	// each collection has separate channel for receiving its events.
	// that channel is served by dedicated goroutine
	ms.collChan = make(map[string]chan<- bson.Raw)
	//
	ms.collBuffers = make(map[string]int)
	// get name of bookmarkColSyncid on sender
	log.Tracef("initSync")
	// init channels before serving routines
	ms.bulkWriteST = make(chan *BulkWriteOp)
	ms.bulkWriteRT = make(chan *BulkWriteOp)
	ms.idleST = make(chan bool, 1) // need buffered for unit tests
	ms.idleRT = make(chan bool, 1) // need buffered in order to init LastSyncId
	// signalling channels
	ms.flush = make(chan struct{})
	ms.idle = make(chan struct{})
}

// initSync creates bulkWriteRT and bulkWriteST chan *BulkWriteOp  used to add bulkWrite operation to buffered channel
// also it initializes collection syncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) initSync(ctx context.Context) error {
	ms.initChannels()
	if err := ms.initRToplog(ctx); err != nil {
		return fmt.Errorf("init oplogRT failed: %s", err)
	}
	// initLastSyncId uses oplogRT so has to follow
	if err := ms.initLastSyncId(ctx); err != nil {
		return fmt.Errorf("init LastSyncId failed: %s", err)
	}
	if err := ms.initSTOplog(ctx); err != nil {
		return fmt.Errorf("init oplogST failed: %s", err)
	}
	return nil
}
