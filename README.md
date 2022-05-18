# yadex
yet another data exchange (mongodb, go, oplog sync dbs)
Keep it

* Simple
* Fast
* Robust

And have fun!

## Algorithm
The idea of this service is to propagate data changes from a sender to a receiver MongoDB server.
All the data in a database is split into two type of sets: RT data (Realtime data) and ST (Historical Data).
It supposes that changes could be made at both the sender and the receiver. 

The class of data defines the way it is synced:
* RT sync is triggered by latest oplog events on RT data update. RT data has an expiration date. If it is expired it will not be sent to the receiver.
It still waits and collects a Bulk of RT updates some RT_DELAY or until MAX_RT_BULK_SIZE achieved before flushing changes into receiver. 
If it fails to flush those Bulk into Receiver - the changes are dropped.
* ST sync tries hard to make sender's and receiver's ST data as equal as it gets. At the beggining it checks what ST collections has been synced before and it it sees oplog is still tracks them it skips cloning. Otherwise it clones all the records from sender to the receiver that are not already there. Then it follows oplog changes to replicate evry changes from sender to the receiver. It is intented to work 24/7 as oplog is capped collections and if it can't restore some collections changes from oplog they could be lost.
* It maintains latest written bookmarks for ST data, so each time when it needs to resume syncing it starts from that bookmark if it can. If it can't, it tries to insert all the documents from collection unordered.

Other features:
* The split to RT/ST is implemented by yaml config file
* It can be dropped and resumed without losses as long as oplog is not capped. Even in that case it will catch up as long as there were not conflicting changes on the receiver side
