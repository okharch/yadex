package mongosync

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"yadex/config"
	"yadex/mdb"
)

// initSync creates bulkWriteRT and bulkWriteST chan *BulkWriteOp  used to add bulkWriteST operation to buffered channel
// also it initializes collection syncId to update sync progress
// bwPut channel is used for signalling there is pending BulkWrite op
// it launches serveBWChan goroutine which serves  operation from those channel and
func (ms *MongoSync) initSync(_ context.Context) error {
	// compile all regexp for Realtime
	ms.rtMatch = compileRegexps(ms.Config.RT)
	ms.stMatch = compileRegexps(ms.Config.ST)
	// data for matching collName => collData
	ms.collData = make(map[string]*CollData)
	ms.coBulkWrite = make(chan struct{}, ms.Config.CoWrite)
	// init channels before serving routines
	ms.bulkWriteST = make(chan *BulkWriteOp, 1)
	ms.bulkWriteRT = make(chan *BulkWriteOp, 1)
	ms.pending = make(map[string]*CollData)
	// get name of bookmarkColSyncid on sender
	log.Tracef("initSync")
	return nil
}

// NewMongoSync returns
func NewMongoSync(ctx context.Context, exchCfg *config.ExchangeConfig) (*MongoSync, error) {
	if err := config.FixExchangeConfig(1, exchCfg); err != nil {
		log.Fatalf("Failed to init exchange: %s", err)
	}
	ms := &MongoSync{Config: exchCfg}
	ms.Config = exchCfg
	log.Infof("Establishing exchange %s...", ms.Name())
	var err error
	ms.senderClient, ms.SenderAvailable, err = mdb.ConnectMongo(ctx, ms.Config.SenderURI)

	if err != nil {
		return nil, fmt.Errorf("failed to establish client for sender: %w", err)
	}
	log.Infof("Sucessfully connected to sender's DB at %s", ms.Config.SenderURI)
	ms.Sender = ms.senderClient.Database(ms.Config.SenderDB)
	ms.receiverClient, ms.ReceiverAvailable, err = mdb.ConnectMongo(ctx, ms.Config.ReceiverURI)
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
	ms.Ready = make(chan bool, 1)
	ms.StatusClean = make(chan bool, 1)
	SendState(ms.Ready, false, "ms.Ready")
	ms.SyncSTDone = make(chan bool, 1)
	for i := range ms.OplogIdle {
		ms.OplogIdle[i] = make(chan bool, 1)
	}
	ms.IsClean = make(chan bool, 1) // this is state channel, must have capacity 1
	return ms, nil
}
