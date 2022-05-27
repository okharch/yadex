package mongosync

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"yadex/config"
)

type (
	CollData struct {
		CollName     string
		Config       *config.DataSync
		OplogClass   OplogClass // class of collection
		Input        Oplog      // dedicated channel for handling input for this collection
		Updated      time.Time  // since when buffer became dirty
		LastSyncId   string     // latest syncId for this collection
		SyncId       string     // latest known syncId for the collection which should be written
		flushTimer   *time.Timer
		flushTimerOn chan bool
		sync.RWMutex
	}
	CollMatch func(coll string) *CollData
)

// getCollData matches collection to the OpLogClass according to configuration and returns 	*CollData .
// It the maintains hash ms.collData to make successive calls for the collection faster
// if it can't find an entry matched for the collection it returns CollData.Config == nil
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
	cdata = &CollData{CollName: collName, Config: cfg}
	if cfg == nil {
		log.Warnf("No found config for %s", collName)
	} else {
		//log.Tracef("config for %s: %v", collName, cfg)
	}
	if realtime {
		cdata.OplogClass = OplogRealtime
	} else {
		cdata.OplogClass = OplogStored
	}
	cdata.flushTimerOn = make(chan bool, 1)
	cdata.flushTimerOn <- false // init flushTimer state
	cdata.flushTimer = &time.Timer{}
	ms.collDataMutex.Lock()
	ms.collData[collName] = cdata
	ms.collDataMutex.Unlock()
	return cdata
}
