package mongosync

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

func (ms *MongoSync) UpdateDirty(ctx context.Context, collData *CollData, delta int, op bson.Raw) {
	//getLock(&collData.RWMutex, false, collData.CollName)
	collData.Lock()
	if collData.Dirty == 0 {
		//getLock(&ms.pendingMutex, false, "ms.pendingMutex")
		ms.pendingMutex.Lock()
		ms.pending[collData.CollName] = getSyncId(op)
		ms.pendingMutex.Unlock()
		//releaseLock(&ms.pendingMutex, false, "ms.pendingMutex")
		collData.Updated = time.Now()
		if collData.Input == nil {
			collData.Input = ms.getCollInput(ctx, collData) // init lastColl.Input, becomes dirty
		}
	}
	collData.Dirty += delta
	if collData.Dirty == 0 {
		//getLock(&ms.pendingMutex, false, "ms.pendingMutex")
		ms.pendingMutex.Lock()
		delete(ms.pending, collData.CollName)
		ms.pendingMutex.Unlock()
		close(collData.Input)
		collData.Input = nil
		//releaseLock(&ms.pendingMutex, false, "ms.pendingMutex")
	}
	collData.Unlock()
	//releaseLock(&collData.RWMutex, false, collData.CollName)
}
