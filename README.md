# yadex
yet another data exchange (mongodb, go, oplog sync dbs)
Keep it

* Simple
* Fast
* Robust

And have fun!

## Algorithm
The idea of this service is to propagate data changes from sender server to receiver server.
All the data in database is split into two tyoe of sets: RT data (Realtime data) and ST (Historical Data).
It supposes that changes could be made at both the sender and the receiver. 
So it propagates changes from the receiver to sender only if sender's lastTimeModifed > receivers lastTimeModifed if it is present.
The class of data defines the way it is synced:
* RT sync is triggered by latest oplog events on RT data update. 
It still waits and collects a Bulk of RT updates some RT_DELAY or until MAX_RT_BULK_SIZE achieved before flushing changes into receiver. 
If it fails to flush those Bulk into Receiver - it drops changes
* ST sync is triggered by oplog changes. It then waits ST_DELAY or UNTIL MAX_ST_BULK_SIZE achieved. Then it checks whether bulkWrite channel is free (length<BUSY_CHANNEL).If it is - it puts those changes into BulkWrite channel.
* It maintains latest written bookmarks for ST data, so each time when it needs to resume syncing it starts from that bookmark if it can. If it can't 
* it tries to insert all the documents from collection unordered

