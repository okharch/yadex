package mongosync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"
	"yadex/utils"
)

// runCollUpdate serves collUpdate channel which
// 1. keep counting collUpdate bytes
// 2. closes channel for collection which don't have dirty bytes
// 3. open channels for collection if they don't have that
// 4. redirects op to that channel
// 5. It finds out that all the collections are clean it broadcasts that change using IsClean (state) channel
func (ms *MongoSync) runCollUpdate(ctx context.Context) {
	defer ms.routines.Done() // runCollUpdate
	totalUpdate := 0
	oldClean := true
	var ppeShowStatus utils.PostponeExecutor
	showStatus := func(status string) {
		ppeShowStatus.Postpone(ctx, func(ctx context.Context) {
			log.Info(status)
		}, time.Millisecond*100)
	}
	// keep last CollData separately for RT and ST
	var zCollData CollData
	lastCollData := [2]*CollData{&zCollData, &zCollData}
	for update := range ms.collUpdate {
		if ctx.Err() != nil {
			return
		}
		lastColl := lastCollData[update.OplogClass]
		// check if it is subject to sync
		if lastColl.CollName != update.CollName {
			lastData := ms.getCollData(update.CollName)
			lastCollData[lastData.OplogClass] = lastData
		}
		if lastColl.Config == nil || lastColl.OplogClass != update.OplogClass {
			continue
		}
		totalUpdate += update.Delta
		if update.Delta < 0 {
			ms.totalBulkWrite -= update.Delta
		}
		clean := totalUpdate == 0
		if oldClean != clean {
			oldClean = clean
			if clean {
				showStatus("msync is idling")
			} else {
				showStatus("msync is replicating changes")
			}
			SendState(ms.IsClean, clean)
		}
		// update coll Dirty
		lastColl.Dirty += update.Delta
		if lastColl.Dirty == 0 {
			lastColl.Lock()
			close(lastColl.Input)
			lastColl.Input = nil
			lastColl.Unlock()
		}
		if clean || update.Op == nil {
			continue
		}
		if lastColl.Input == nil {
			ms.getCollInput(ctx, lastColl)
		}

		lastColl.Lock()
		cancel := CancelSend(ctx, lastColl.Input, update.Op)
		lastColl.Unlock()
		if cancel {
			break
		}
	}
}
