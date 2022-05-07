package mongosync

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

// initIdle creates channels collCountChan and idleChan used to implement checkIdle facility
func (ms *MongoSync) initIdle() {
	// this channel is used for non-blocking communication info about pending buffers in collection's channels
	ms.collCountChan = make(chan collCount, 1)
	ms.collCount = make(map[string]int)
	ms.idleChan = make(chan struct{}, 1)
	ms.Sync.Add(1)
	go ms.serveCollCount()
}

// serveCollCount receives info from collCount channel about pending buffers for each collection
func (ms *MongoSync) serveCollCount() {
	defer ms.Sync.Done()
	for cc := range ms.collCountChan {
		if cc.count == 0 {
			// remove collection if no pending updates
			delete(ms.collCount, cc.coll)
			if cc.coll == ms.collMax {
				// refresh Max
				ms.collMaxCount = 0
				for coll, count := range ms.collCount {
					if count > ms.collMaxCount {
						ms.collMax = coll
						ms.collMaxCount = count
					}
				}
			}
			continue
		}
		if cc.count > ms.collMaxCount {
			ms.collMax = cc.coll
			ms.collMaxCount = cc.count
		}
		ms.collCount[cc.coll] = cc.count
	}
}

// this is to trigger NoInput event for MongoSync housekeeping
// it checks also whether there is no pending buffers for receiver
// if there is no, then it send  Idle event into IdleChannel
// which WaitIdle expects before returning
func (ms *MongoSync) checkIdle() {
	// non-blocking flush idleChan if it is dirty to make sure that
	// ms.idleChan <- struct{}{} (below) will not block
	select {
	case <-ms.idleChan:
	default:
	}
	if ms.collMaxCount > 0 {
		// this will refreshMax upon flushing
		ms.collChan[ms.collMax] <- nil
	} else {
		// checkIdle() has been called from oplog timeout and found out that there is no pending buffers to send to the rceiver
		// in this case send event to idleChan, it will not be blocking as idleChan is buffered and was flushed above
		ms.idleChan <- struct{}{}
	}
}

// WaitIdle detects situation when there is no pending input/output
func (ms *MongoSync) WaitIdle() {
	<-ms.idleChan // wait there is no input
	ms.putBwOp(nil)
}
