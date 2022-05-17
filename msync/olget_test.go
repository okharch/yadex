package mongosync

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

// BenchmarkLookup 150 ns/op
func BenchmarkLookup(b *testing.B) {
	m := bson.M{"ns": bson.M{"coll": "test1", "db": "testdb"}}
	data, _ := bson.Marshal(m)
	r := bson.Raw(data)
	var coll string
	for i := 0; i < b.N; i++ {
		coll = r.Lookup("ns", "coll").StringValue()
	}
	log.Infof("coll:%s", coll)
}

// BenchmarkUnmarshal 2191 ns/op
func BenchmarkUnmarshal(b *testing.B) {
	m := bson.M{"ns": bson.M{"coll": "test1", "db": "testdb"}}
	data, _ := bson.Marshal(m)
	var coll string
	m1 := bson.M{}
	for i := 0; i < b.N; i++ {
		_ = bson.Unmarshal(data, m1)
		coll = m1["ns"].(bson.M)["coll"].(string)
	}
	log.Infof("coll:%s", coll)
}

// BenchmarkBsonM 15 ns/op
func BenchmarkBsonM(b *testing.B) {
	m := bson.M{"ns": bson.M{"coll": "test1", "db": "testdb"}}
	data, _ := bson.Marshal(m)
	var coll string
	m1 := bson.M{}
	_ = bson.Unmarshal(data, m1)
	for i := 0; i < b.N; i++ {
		coll = m1["ns"].(bson.M)["coll"].(string)
	}
	log.Infof("coll:%s", coll)
}

func TestGetOpColl(t *testing.T) {
	op := createOp(t, bson.M{"ns": bson.M{"coll": "test"}})
	require.Equal(t, "test", getOpColl(op))
	op = createOp(t, bson.M{"ns": bson.M{"coll1": "test"}})
	require.Equal(t, "", getOpColl(op))
	op = createOp(t, bson.M{})
	require.Equal(t, "", getOpColl(op))
}
