package oplog

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)
type (
	Oplog = <-chan bson.Raw
	// enum type to define what kind of operation is BulkWriteOp: (OpLogUnknown, OpLogUnordered, OpLogOrdered, OpLogDrop, OpLogCloneCollection)
	OpLogType = int
)

// enum OpLogType
const (
	OpLogUnknown   OpLogType = iota // we don't know how to handle this operation
	OpLogUnordered                  // BulkWrite should have ordered:no option
	OpLogOrdered                    // don't use unordered option, default ordered is true
	OpLogDrop                       // indicates current operation is drop
)

// getChangeStream tries to resume sync from last successfully committed syncId.
func getChangeStream(ctx context.Context, db *mongo.Database, syncId string) (changeStream *mongo.ChangeStream, err error) {
	// add option so mongo provides FullDocument for update event
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if syncId != "" {
		resumeToken := &bson.M{"_data": syncId}
		opts.SetResumeAfter(resumeToken)
	}
	var pipeline mongo.Pipeline
	// start watching oplog before cloning collections
	return db.Watch(ctx, pipeline, opts)
}

// GetDbOpLog returns channel with oplog records
func GetDbOpLog(ctx context.Context, db *mongo.Database, syncId string) (Oplog, error) {
	changeStream, err := getChangeStream(ctx, db, syncId)
	if err != nil {
		return nil, err
	}
	ch := make(chan bson.Raw)
	go func() {
		defer close(ch)
		for {
			for changeStream.Next(ctx) {
				ch <- changeStream.Current
			}
			err = changeStream.Err()
			if errors.Is(err, context.Canceled) {
				close(ch)
				log.Info("process oplog is gracefully shutdown")
				return
			}
			if err != nil {
				log.Errorf("changelog error: %s", err)
			}
		}
	}()
	return ch, nil
}

// GetNS returns db and collection name frop op
func GetNS(op bson.Raw) (db string, coll string) {
	ns := op.Lookup("ns")
	var nsm bson.M
	err := bson.Unmarshal(ns.Value, &nsm)
	if err != nil {
		return
	}
	dbv, ok := nsm["db"]
	if ok {
		db, ok = dbv.(string)
	}
	collv, ok := nsm["coll"]
	if ok {
		coll, _ = collv.(string)
	}
	return
}

// returns value for specified field name from document of bson.Raw
func GetValue(doc bson.Raw, fieldName string) bson.Raw {
	return doc.Lookup(fieldName).Value
}

// GetWriteModel for current oplog decodes into OpLogType and WriteModel
func GetWriteModel(op bson.Raw) (opLogType OpLogType, m mongo.WriteModel) {
	opTypeName := op.Lookup("operationType").StringValue()
	opLogType = OpLogOrdered
	switch opTypeName {
	case "insert":
		m = &mongo.InsertOneModel{Document: GetValue(op, "fullDocument")}
		opLogType = OpLogUnordered
	case "update", "replace": //,"insert":
		upsert := true
		m = &mongo.ReplaceOneModel{Upsert: &upsert, Filter: GetValue(op, "documentKey"), Replacement: GetValue(op, "fullDocument")}
	case "delete":
		m = &mongo.DeleteOneModel{Filter: GetValue(op, "documentKey")}
	case "drop":
		opLogType = OpLogDrop
	default:
		opLogType = OpLogUnknown
	}
	return
}
