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
	"time"
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
	ctx                      context.Context
	cancelFunc               context.CancelFunc
	Sender, Receiver         *mongo.Database
	senderClient             *mongo.Client
	receiverClient           *mongo.Client
	senderAvailable          chan bool         // this channel follows availability of the sender server
	receiverAvailable        chan bool         // this channel follows availability of the receiver server
	bulkWriteRT, bulkWriteST chan *BulkWriteOp // channels for RT and ST BulkWrites
	Config                   *config.ExchangeConfig
	routines                 sync.WaitGroup // housekeeping of runSync. it is zero on exit
	IsClean                  chan bool      // broadcast the change of dirty state, must have capacity 1
	ready                    chan bool      // it send true when msync is ready to process
	// send true if sync becomes dirty,
	// send false so runDirt can check  whether sync is clean
	dirty        chan bool
	collBookmark map[string]time.Time
	// syncId collection to store sync progress bookmark for ST collections.
	// When it starts sync session for an ST collection again it tries to resume syncing from that bookmark.
	syncId     *mongo.Collection
	lastSyncId string
	// this signal is sent right before oplog requires blocking log.Next statement so before getting blocked it sends this signal.
	collMatch     CollMatch // config, realtime nil and false if not found
	collChanMutex sync.RWMutex
	collsSyncDone chan bool        // SyncCollections sends signal here when it is done
	collChan      map[string]Oplog // any collection has it's dedicated channel with dedicated goroutine.
	// checkIdle facility, see checkIdle.go
	countBulkWriteRT     sync.WaitGroup // this is used to prevent ST BulkWrites clash over RT line
	rtUpdated, stUpdated map[string]struct{}
	pendingBuffers       int // pending bytes in collection's buffers
	collBuffersMutex     sync.RWMutex
	pendingBulkWrite     int // total of bulkWrite buffers queued at bulkWriteST channel
	totalBulkWrite       int // this counts total bytes flushed to the receiver
	bulkWriteMutex       sync.RWMutex
	bwLog                [bwLogSize]BWLog
	bwLogIndex           int
	// flush signal when BulkWrite channel is idling
	flush chan struct{}
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig, ready chan bool) (*MongoSync, error) {
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
	ms.ready = ready
	return ms, nil
}

func (ms *MongoSync) getTotalBulkWrite() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	return ms.totalBulkWrite
}

// Run launches synchronization between sender and receiver for established MongoSync object.
// Use cancel (ctx) func to stop synchronization
// It is used after Stop in a case of Exchange is not available (either sender or receiver)
// So it creates all channels and trying to resume from the safe point
func (ms *MongoSync) Run(ctx context.Context) {
	exCtx, exCancel := context.WithCancel(ctx)
	defer exCancel()
	senderAvail := false
	receiverAvail := false
	oldAvail := false
	var rsyncWG sync.WaitGroup
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
				// create func to put BulkWriteOp into channel. It deals appropriate both with RT and ST
				if err := ms.initSync(ctx); err != nil {
					exCancel()
					log.Fatalf("init failed: %s", err)
				}
				SendState(ms.ready, true, "ms.ready")
				rsyncWG.Add(1)
				go func() {
					defer rsyncWG.Done()
					ms.runSync(exCtx)
				}()
			} else {
				SendState(ms.ready, false, "ms.ready")
				log.Warnf("Exchange unavailable, stopping %s... ", ms.Name())
				exCancel()
				rsyncWG.Wait()
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
	// you can count this using
	// git grep ms.routines.|grep -v _test|grep -v SyncCollections|grep -v  getOplog|grep -v runSToplog
	ms.routines.Add(2)
	// show avg speed of BulkWrite ops to the log
	go ms.showSpeed(ctx) // optional, comment out if not needed
	go ms.runFlush(ctx)  // flush signal server
	log.Tracef("runSync running servers")
	<-ctx.Done()
	log.Infof("waiting to shutdown sync exchange %s", ms.Name())
	ms.countBulkWriteRT.Wait() // wait before shutdown
	ms.routines.Wait()
}

func (ms *MongoSync) initChannels(ctx context.Context) {
	ms.IsClean = make(chan bool, 1) // this is state channel, must have capacity 1
	// signalling channels
	ms.flush = make(chan struct{})
	ms.collsSyncDone = make(chan bool, 1)
	ms.dirty = make(chan bool) // send false if there is a chance to be IsClean
	ms.routines.Add(1)         // runDirt
	go ms.runDirt(ctx)         // serve dirty channel
	// init channels before serving routines
	ms.bulkWriteST = make(chan *BulkWriteOp)
	ms.bulkWriteRT = make(chan *BulkWriteOp)
	ms.routines.Add(2) // runRTBulkWrite, runSTBulkWrite
	go ms.runRTBulkWrite(ctx)
	go ms.runSTBulkWrite(ctx)

	// each collection has separate channel for receiving its events.
	// that channel is served by dedicated goroutine
	ms.collChan = make(map[string]Oplog)
	//
	ms.rtUpdated = make(map[string]struct{})
	ms.stUpdated = make(map[string]struct{})
	ms.collBookmark = make(map[string]time.Time)
	// get name of bookmarkColSyncid on sender
	log.Tracef("initSync")

}

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}

// initSync creates bulkWriteRT and bulkWriteST chan *BulkWriteOp  used to add bulkWrite operation to buffered channel
// also it initializes collection syncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) initSync(ctx context.Context) error {
	ms.totalBulkWrite = 0
	ms.initChannels(ctx)
	if len(ms.Config.RT) > 0 {
		ms.initRToplog(ctx)
	}
	if len(ms.Config.ST) > 0 {
		ms.initSTOplog(ctx)
	}
	return nil
}
