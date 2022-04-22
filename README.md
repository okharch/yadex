# yadex
yet another data exchange (mongodb, go, oplog sync dbs)
Keep it

* Simple
* Fast
* Robust

An have fun!

Algorithm
The idea of this service is to sync offline/realtime some database from sender server to receiver server.
All the data in database is split into two sets: RT data (Realtime data) and ST (Historical Data).
The class of data defines the way it is synced:
* RT sync is triggered by latest oplog events on RT data update. It still waits and collect a Bulk of RT updates some RT_DELAY or until MAX_RT_BULK_SIZE achieved before flushing changes into receiver. If it fails to flush those Bulk into Receiver - it drops changes
* ST sync is triggered by oplog changes. It then waits ST_DELAY or UNTIL MAX_ST_BULK_SIZE achieved. Then it checks whether bulkWrite channel is free (length<BUSY_CHANNEL).If it is - it puts those changes into BulkWrite channel.
* It maintains latest written bookmarks for ST data, so each time when it needs to resume syncing it starts from that bookmark if it can. If it can't 
* it tries to insert all the documents from collection unordered, receives all the _IDs which triggered primary key already exists, 
* checks for those whether they have been updated (lastTimeModifed > creationTime). 
* sends updated records using updateOne({_id:id, lastTimeModifed<local.lastTimeModifed},{'$set':$document})

