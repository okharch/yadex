package mongosync

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"testing"
)

// getNS extracts collection's name from op(log)
func getNS(op bson.Raw) (db, coll string) {
	doc := op.Lookup("ns")
	if len(doc.Value) == 0 {
		return "", ""
	}
	d := doc.Document()
	db = d.Lookup("db").StringValue()
	coll = d.Lookup("coll").StringValue()
	return
}

func getOpColl(op bson.Raw) string {
	return getString(op.Lookup("ns", "coll"), "")
}

func getString(v bson.RawValue, ifEmpty string) string {
	if len(v.Value) == 0 {
		return ifEmpty
	}
	return v.StringValue()

}

// getOpName shows what operation coming from oplog
var lastId string
var lastIdMutex sync.Mutex
var diff [255]bool

func getDiffId(newId string) string {
	var buff bytes.Buffer
	if newId == "" {
		return "EMPTY"
	}
	lastIdMutex.Lock()
	defer lastIdMutex.Unlock()
	if lastId != "" {
		for i, c := range []byte(newId) {
			if i >= len(lastId) || c != lastId[i] {
				diff[i] = true
				buff.Write([]byte{c})
			}
		}
	} else {
		buff.WriteString(newId)
	}
	lastId = newId
	result := buff.String()
	if result == "" {
		result = "SAME"
	}
	return result
}

func getOpName(op bson.Raw) string {
	if op == nil {
		return "nil"
	}
	showId := getDiffId(getSyncId(op))
	db, coll := getNS(op)
	opTypeName := getString(op.Lookup("operationType"), "empty op")
	return db + "." + coll + ":" + opTypeName + " @ " + showId
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

// createOp is used by various unit tests to convert bson.M into bson.Raw
func createOp(t *testing.T, op bson.M) (r bson.Raw) {
	r, err := bson.Marshal(op)
	require.NoError(t, err)
	return
}
