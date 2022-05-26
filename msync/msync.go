package mongosync

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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

type Oplog chan bson.Raw

const bwLogSize = 256

type CollUpdate struct {
	CollName   string
	Op         bson.Raw
	OplogClass OplogClass
	Delta      int
}

type MongoSync struct {
	ctx               context.Context
	cancelFunc        context.CancelFunc
	Sender, Receiver  *mongo.Database
	senderClient      *mongo.Client
	receiverClient    *mongo.Client
	SenderAvailable   chan bool // this channel follows availability of the sender server
	ReceiverAvailable chan bool // this channel follows availability of the receiver server
	SyncSTDone        chan bool // sends true/ false depending on what is the state of SyncCollections
	Config            *config.ExchangeConfig
	routines          sync.WaitGroup // housekeeping of runSync. it is zero on exit
	IsClean           chan bool      // broadcast the change of collUpdate state, must have capacity 1
	StatusClean       chan bool      // broadcast the change of collUpdate state, must have capacity 1
	Ready             chan bool      // sends true when msync is Ready to proceed (db connected)
	OplogIdle         [3]chan bool   // send true when appropriate channel waits for input, false otherwise
	OplogClean        [3]chan bool   // send true when appropriate channel waits for input, false otherwise
	// syncId collection to store sync progress bookmark for ST collections.
	// When it starts sync session for an ST collection again it tries to resume syncing from that bookmark.
	syncId     *mongo.Collection
	lastSyncId string
	// this signal is sent right before oplog requires blocking log.Next statement so before getting blocked it sends this signal.
	bwLog                           [bwLogSize]BWLog
	bwLogIndex                      int
	rtMatch, stMatch                collMatch
	collDataMutex                   sync.RWMutex
	bulkWriteMutex                  sync.RWMutex
	bulkWriteRT, bulkWriteST        chan *BulkWriteOp // channels for Realtime and ST BulkWrites
	bulkWriteRTCount                sync.WaitGroup
	coBulkWrite                     chan struct{} // buffered channel used as semaphor limiting count of simultaneous BulkWrites
	totalBulkWrite                  int
	collData                        map[string]*CollData
	lastBookmarkIdMutex             sync.Mutex
	lastBookmarkId, lastBookmarkInc primitive.DateTime
	pendingMutex                    sync.RWMutex
	// this two channel is for keeping Pending state when BulkWrite is being done
	ChangeColl chan ChangeColl
	Pending    chan map[string]string
	// flushOnIdle signal when BulkWrite channel is idling
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}
