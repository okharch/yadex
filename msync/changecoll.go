package mongosync

import "context"

// this record tells ChangeColl handler to update pending collections on arrival changelog records for another collection and also it is
//used to signal that some SyncId was flushed to mongo
type (
	CollSyncIdList struct {
		CollName string
		SyncId   string
		Next     *CollSyncIdList
	}
	ChangeColl struct {
		CollName  string
		SyncId    string
		BulkWrite bool //
	}
)

func (ms *MongoSync) runChangeColl(ctx context.Context) {
	defer ms.routines.Done()
	var head, tail *CollSyncIdList
	clean := true
	for cc := range ms.ChangeColl {
		if ctx.Err() != nil {
			return
		}
		if cc.BulkWrite {
			// we should remove all the entries with the same CollName at left and
			// return to the pending channel names and syncId for the other collections at left
			pending := make(map[string]string)
			prev := head
			for current := head; current != nil && current.SyncId <= cc.SyncId; current = current.Next {
				if current.CollName == cc.CollName {
					// have to remove cur from list
					if head == current {
						// we are at the head yet, remove it
						head = current.Next
						if head == nil {
							// we have the only element, remove it
							tail = nil
							if !clean {
								clean = true
								SendState(ms.IsClean, clean)
							}
						} else {
							prev = head
						}
					} else {
						// remove current
						prev.Next = current.Next
					}
				} else {
					// need to add CollName to pending if it is not already there
					_, ok := pending[current.CollName]
					if !ok {
						pending[current.CollName] = current.SyncId
					}
					prev = current
				}
			}
			ms.Pending <- pending
		} else {
			// just add new elem to the tail of the list
			newElem := &CollSyncIdList{CollName: cc.CollName, SyncId: cc.SyncId}
			if tail == nil {
				head = newElem
			} else {
				tail.Next = newElem
			}
			tail = newElem
			if clean {
				clean = false
				SendState(ms.IsClean, clean)
			}
		}
	} // for cc := range ms.ChangeColl
}
