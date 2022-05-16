package mongosync

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// getCollName extracts collection's name from op(log)
func getCollName(op bson.Raw) string {
	return getString(op.Lookup("ns", "coll"), "")
}

func getString(v bson.RawValue, ifEmpty string) string {
	if len(v.Value) == 0 {
		return ifEmpty
	}
	return v.StringValue()

}

// getOpName shows what operation coming from oplog
func getOpName(op bson.Raw) string {
	if op == nil {
		return "nil"
	}
	syncId := getSyncId(op)
	coll := getCollName(op)
	opTypeName := getString(op.Lookup("operationType"), "empty op")
	return coll + ":" + opTypeName + " @ " + syncId
}

// getSyncId extracts _id._data portion of op(log)
func getSyncId(op bson.Raw) string {
	return getString(op.Lookup("_id", "_data"), "")
}

// getWriteModel decodes op(log) into OpLogType and WriteModel
func getWriteModel(op bson.Raw) (opLogType OpLogType, model mongo.WriteModel) {
	opTypeName := getString(op.Lookup("operationType"), "")
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

//// createOp is used by various unit tests to convert bson.M into bson.Raw
//func createOp(t *testing.T, op bson.M) (r bson.Raw) {
//	r, err := bson.Marshal(op)
//	require.NoError(t, err)
//	return
//}
