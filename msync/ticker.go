package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
)

func (ms *MongoSync) runStatus(ctx context.Context) {
	defer ms.routines.Done()
	for {
		select {
		case <-ctx.Done():
			log.Tracef("context expited")
			return
		case idle := <-ms.OplogIdle[OplogRealtime]:
			log.Tracef("ms.OplogIdle[OplogRealtime]:%v", idle)
		case idle := <-ms.OplogIdle[OplogStored]:
			log.Tracef("ms.OplogIdle[OplogStored]:%v", idle)
		case clean := <-ms.StatusClean:
			log.Tracef("ms.Clean:%v", clean)
		}
	}
}
