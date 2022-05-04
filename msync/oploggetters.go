package mongosync

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// getColl extracts collection's name from op(log)
func getColl(op bson.Raw) string {
	return op.Lookup("ns", "coll").StringValue()
}

// getSyncId extracts _id._data portion of op(log)
func getSyncId(op bson.Raw) string {
	return op.Lookup("_id", "_data").StringValue()
}

// getWriteModel decodes op(log) into OpLogType and WriteModel
func getWriteModel(op bson.Raw) (opLogType OpLogType, model mongo.WriteModel) {
	opTypeName := op.Lookup("operationType").StringValue()
	opLogType = OpLogOrdered
	switch opTypeName {
	case "insert":
		model = &mongo.InsertOneModel{Document: op.Lookup("fullDocument").Value}
		opLogType = OpLogUnordered
	case "update", "replace": //,"insert":
		upsert := true
		model = &mongo.ReplaceOneModel{Upsert: &upsert, Filter: op.Lookup("documentKey").Value, Replacement: op.Lookup("fullDocument").Value}
	case "delete":
		model = &mongo.DeleteOneModel{Filter: op.Lookup("documentKey").Value}
	case "drop":
		opLogType = OpLogDrop
	default:
		opLogType = OpLogUnknown
	}
	return opLogType, model
}
