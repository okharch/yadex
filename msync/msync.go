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

type CollUpdate struct {
	CollName   string
	Op         bson.Raw
	OplogClass OplogClass
	Delta      int
}

type MongoSync struct {
	ctx                    context.Context
	cancelFunc             context.CancelFunc
	Sender, Receiver       *mongo.Database
	senderClient           *mongo.Client
	receiverClient         *mongo.Client
	senderAvailable        chan bool         // this channel follows availability of the sender server
	receiverAvailable      chan bool         // this channel follows availability of the receiver server
	bulkWriteRT, bulkWrite chan *BulkWriteOp // channels for Realtime and ST BulkWrites
	Config                 *config.ExchangeConfig
	routines               sync.WaitGroup // housekeeping of runSync. it is zero on exit
	IsClean                chan bool      // broadcast the change of collUpdate state, must have capacity 1
	ready                  chan bool      // it send true when msync is ready to proceed
	// send count of collUpdate bytes +(plus) is queued, -(minus) is flushed
	collUpdate chan CollUpdate
	// syncId collection to store sync progress bookmark for ST collections.
	// When it starts sync session for an ST collection again it tries to resume syncing from that bookmark.
	syncId     *mongo.Collection
	lastSyncId string
	// this signal is sent right before oplog requires blocking log.Next statement so before getting blocked it sends this signal.
	collsSyncDone    chan bool // SyncCollections sends signal here when it is done
	bwLog            [bwLogSize]BWLog
	bwLogIndex       int
	rtMatch, stMatch collMatch
	collDataMutex    sync.RWMutex
	bulkWriteMutex   sync.RWMutex
	totalBulkWrite   int
	collData         map[string]*CollData
	// flushOnIdle signal when BulkWrite channel is idling
}

var allRecords = bson.D{} // used for filter parameter where there is no need to filter

// CollSyncIdDbName is the name of database where we store collections for different exchanges (one collection per exchange)
// collection stores last sync_id for each individual collection being synced - so it can resume sync from that point
const CollSyncIdDbName = "mongoSync"

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig, ready chan bool) (*MongoSync, error) {
	ms := &MongoSync{Config: exchCfg}
	config.FixExchangeConfig(1, exchCfg)
	if exchCfg.CoWrite < 1 {
		panic("wrong value for CoWrite!")
	}
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
	bookmarkColSyncIdName := strings.Join([]string{ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB},
		"-")
	reNotId := regexp.MustCompile("[^a-zA-Z0-9]+")
	bookmarkColSyncIdName = reNotId.ReplaceAllLiteralString(bookmarkColSyncIdName, "-")
	ms.syncId = ms.senderClient.Database(CollSyncIdDbName).Collection(bookmarkColSyncIdName)
	ms.ready = ready
	return ms, nil
}

func (ms *MongoSync) Name() string {
	return fmt.Sprintf("%s.%s => %s.%s", ms.Config.SenderURI, ms.Config.SenderDB, ms.Config.ReceiverURI, ms.Config.ReceiverDB)
}
