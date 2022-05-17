package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"
)

// initRToplog just starts to follow changes from the current tail of the oplog
func (ms *MongoSync) initRToplog(ctx context.Context) {
	oplog, err := ms.getOplog(ctx, ms.Sender, "", time.Now(), "RT")
	if err != nil {
		log.Fatalf("failed to init RT oplog: %s", err)
	}
	ms.routines.Add(1) // runRToplog
	go ms.runRToplog(ctx, oplog)
}

// runRToplog handles incoming oplogRT entries from oplogRT channel.
// It finds out which collection that oplogRT op belongs.
// If a channel for handling that collection has not been created,
// it calls getCollChan func to create that channel.
// Then it redirects oplogRT op to that channel.
func (ms *MongoSync) runRToplog(ctx context.Context, oplog Oplog) {
	defer ms.routines.Done() // runRToplog
	for op := range oplog {
		// we deal with the same db all the time,
		// it is enough to dispatch based on collName only
		collName := getOpColl(op)
		if collName == "" {
			continue
		}
		// check if it is synced
		config, rt := ms.collMatch(collName)
		if rt {
			// now get handling channel for that collection and direct op to that channel
			ch := ms.getCollChan(ctx, collName, config, rt)
			ch <- op
		}
	}
	log.Debug("runRToplog gracefully shutdown on cancelled context")
}
