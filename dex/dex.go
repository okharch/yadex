package dex

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"sync"
	"time"
	"yadex/oplog"
)

var ordered = false
var ignore_order = &options.InsertManyOptions{Ordered: &ordered}
type (
	Dex struct {
		sender, receiver *mongo.Client
		oplog oplog.Oplog
		collPriority CollPriority
		senderDB, receiverDB, mirrorDB *mongo.Database
		sync.Mutex // to prevent fight between mirroring and realtime updates
	}
	CollPriority []regexp.Regexp
	// this is postponed operation of modification which is put into prioritized channel
	// it arrives as an input for goroutine inside GetBulkWriteOpChan(), see switch there to find out how it is processed
	BulkWriteOp struct {
		OpType oplog.OpLogType
		Models  []mongo.WriteModel
		Updated time.Time
	}
)
func NewDex(sender, receiver *mongo.Client, senderDBName, receiverDBName string) *Dex {
	return &Dex{
		sender: sender,
		receiver: receiver,
		senderDB : sender.Database(senderDBName),
		receiverDB : receiver.Database(receiverDBName),
	}
}

func (dex *Dex) GetPriority(coll string) int {
	for i, re := range dex.collPriority {
		if re.Match([]byte(coll)) {
			return i
		}
	}
	return -1
}

const flushTime = time.Millisecond*100
func addOp(bw *BulkWriteOp, logType oplog.OpLogType, model mongo.WriteModel) *BulkWriteOp {
	if bw == nil {
		bw = &BulkWriteOp{}
	}
	bw.OpType = logType
	bw.Models = append(bw.Models, model)
	if bw.Updated.IsZero() {
		bw.Updated = time.Now()
	}
	return bw
}

func (dex *Dex) flush(ctx context.Context, coll string, op *BulkWriteOp) {
	dex.Lock()
	defer dex.Unlock()
	err, _ := dex.receiverDB.Collection(coll).BulkWrite(ctx,op.Models)
	if err != nil {
		log.Errorf("failed to write %d records into receiver.%s : %s", coll, err)
	}
	err, _ = dex.mirrorDB.Collection(coll).BulkWrite(ctx,op.Models)
	if err != nil {
		log.Errorf("failed to write %d records into mirror %s.%s : %s", dex.mirrorDB.Name(), coll, err)
	}
	op.Models = nil
	op.Updated = time.Time{} // zero
}

type Name     struct{ Name string }

func listNames(list []Name) []string {
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.Name
	}
	return result
}

func listCollections(ctx context.Context, db *mongo.Database) []string {
	c, err := db.ListCollections(ctx, bson.M{})
	if err != nil {
		log.Fatalf("Can't list collections: %s", err)
	}
	var lcResult []Name
	err = c.All(ctx, &lcResult)
	if err != nil {
		log.Fatalf("Can't list collections: %s", err)
	}
	return listNames(lcResult)
}


// RunMirror finds difference between sender and mirror and sends delta to the receiver and updates the mirror
func (dex *Dex) RunMirror(ctx context.Context) {
	// get list of synced collection from sender
	// get intersection for this list from the receiver
	// download data for the synced collection from the receiver
	// mark it as mirrored in mirrored collection
	// calculate the delta for each collection and update the receiver and the mirror accordingly
	receiverCollections := listCollections(ctx, dex.receiverDB)
	rColl := make(map[string]bool, len(receiverCollections))
	for _, coll := range receiverCollections {
		rColl[coll] = true
	}
	var mirroredColl [] struct {
		Name string `bson:"_id"`
	}
	mirrored := dex.mirrorDB.Collection("mirrored")
	c, err := mirrored.Find(ctx, bson.M{})
	if err == nil {
		_ = c.All(ctx, &mirroredColl)
	}
	mColl := make(map[string]bool)
	for _, coll := range mirroredColl {
		mColl[coll.Name] = true
	}
	for _, coll := range listCollections(ctx, dex.senderDB) {
		if dex.GetPriority(coll) == -1 {
			continue
		}
		if rColl[coll] && !mColl[coll] {
			log.Infof("cloning collection from receiver into mirror %s...", coll)
			if err := cloneCollection(ctx, dex.receiverDB, dex.mirrorDB, coll, 1000); err != nil {
				log.Errorf("failed to clone collection %s into mirror: %s", coll, err)
			} else {
				mirrored.InsertOne(ctx, bson.M{"_id":coll})
			}
		}
	}
}

type IdDoc struct {
	Id primitive.ObjectID `bson:"_id"`
}
func getDexId(ctx context.Context, db *mongo.Database) string {
	coll := db.Collection("dex")
	var id IdDoc
	err := coll.FindOne(ctx, bson.M{}).Decode(&id)
	if err == nil {
		return id.Id.Hex()
	}
	if err == mongo.ErrNoDocuments {
		ir, err := coll.InsertOne(ctx, bson.M{})
		if err == nil {
			return ir.InsertedID.(primitive.ObjectID).Hex()
		}
	}
	log.Fatalf("failed to get dex id for the receiver: %s", err)
	return ""
}

func (dex *Dex) Start(ctx context.Context, senderDBName, receiverDBName string) {
	dexId := getDexId(ctx, dex.receiverDB)
	mirrorDBName := "mirror-" + dexId
	dex.mirrorDB = dex.receiver.Database(mirrorDBName)
	opLog, err := oplog.GetDbOpLog(ctx,dex.senderDB,"")
	if err != nil {
		log.Fatalf("failed to open change stream, db setup for oplog needed: %s", err)
	}
	colBW := make(map[string]*BulkWriteOp)
	for op := range opLog {
		// propagate
		db, coll := oplog.GetNS(op)
		if db != senderDBName {
			continue
		}
		priority := dex.GetPriority(coll)
		if priority == -1 {
			continue
		}
		opLogType, wModel := oplog.GetWriteModel(op)
		bw := colBW[coll]
		if bw != nil && bw.OpType != opLogType {
			dex.flush(ctx, coll, bw)
			bw.Updated = time.Now()
		}
		colBW[coll] = addOp(bw, opLogType, wModel)
		for coll, bw := range colBW {
			if !bw.Updated.IsZero() && time.Since(bw.Updated) > flushTime || len(bw.Models) >= 100 {
				dex.flush(ctx, coll, bw)
			}
		}

	}
}

// cloneCollection clones collection by sending bulk write ops into out channel
func cloneCollection(ctx context.Context, receiver, mirror *mongo.Database, collName string, maxBulkCount int) error {
	targetCol := mirror.Collection(collName)
	sourceCol := receiver.Collection(collName)
	c, err := sourceCol.Find(ctx, bson.M{})
	if err == mongo.ErrNoDocuments {
		return nil
	}
	docs := make([]interface{}, maxBulkCount)
	i := 0
	for c.Next(ctx) {
		docs[i] = c.Current
		i++
		if i == len(docs) {
			// insert ignoring duplicate key errors
			_, err := targetCol.InsertMany(ctx,docs, ignore_order)
			if err != nil {
				return err
			}
			i = 0
		}
	}
	if err := c.Err(); err != nil {
		return err
	}
	_, err = targetCol.InsertMany(ctx,docs[:i])
	return err
}
