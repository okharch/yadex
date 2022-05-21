package mongosync

import (
	"sync"
	"time"
	"yadex/config"
)

type (
	CollData struct {
		CollName     string
		Config       *config.DataSync
		OplogClass   OplogClass   // class of collection
		Input        Oplog        // dedicated channel for handling input for this collection
		Dirty        int          // counter for dirty bytes still not sent
		LastSyncId   string       // latest syncId for this collection
		PrevBookmark SyncBookmark // latest bookmark. before writing new - delete it
		Flushed      time.Time
		Writing      sync.WaitGroup // counters for writing, prevents  ordered ops being written simultaneously
		sync.RWMutex
	}
	CollMatch func(coll string) *CollData
)

// getCollData matches collection to the OpLogClass according to configuration and returns 	*CollData .
//It the maintains hash ms.collData to make successive calls for the collection faster
// if it can't find an entry matched for the collection it returns config == nil
func (ms *MongoSync) getCollData(collName string) *CollData {
	// special case, if collName == "" - close all channels
	// try cache first
	ms.collDataMutex.RLock()
	cdata, ok := ms.collData[collName]
	ms.collDataMutex.RUnlock()
	if ok {
		return cdata
	}
	// look for Realtime
	cm := []byte(collName)
	cfg := findEntry(ms.rtMatch, cm)
	var realtime bool
	if cfg != nil {
		realtime = true
	} else {
		// look for ST
		cfg = findEntry(ms.stMatch, cm)
	}
	// cache the request, so next time it will be faster
	cdata = &CollData{CollName: collName, Config: cfg, Flushed: time.Now()}
	if realtime {
		cdata.OplogClass = OplogRealtime
	} else {
		cdata.OplogClass = OplogStored
	}
	ms.collDataMutex.Lock()
	ms.collData[collName] = cdata
	ms.collDataMutex.Unlock()
	return cdata
}
