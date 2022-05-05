package mongosync

import (
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
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

// createOp is used by various unit tests to convert bson.M into bson.Raw
func createOp(t *testing.T, op bson.M) (r bson.Raw) {
	r, err := bson.Marshal(op)
	require.NoError(t, err)
	return
}
