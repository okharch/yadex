package mongosync

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChangeColl(t *testing.T) {
	ms := &MongoSync{ChangeColl: make(chan ChangeColl), Pending: make(chan map[string]string)}
	ctx := context.TODO()
	ms.routines.Add(1)
	go ms.runChangeColl(ctx)
	ms.ChangeColl <- ChangeColl{CollName: "t1", SyncId: "1"}
	ms.ChangeColl <- ChangeColl{CollName: "t2", SyncId: "2"}
	ms.ChangeColl <- ChangeColl{CollName: "t3", SyncId: "3"}
	ms.ChangeColl <- ChangeColl{CollName: "t1", SyncId: "4"}

	ms.ChangeColl <- ChangeColl{CollName: "t1", SyncId: "4", BulkWrite: true}
	pending := <-ms.Pending
	require.Equal(t, 2, len(pending))
	require.Equal(t, "2", pending["t2"])
	require.Equal(t, "3", pending["t3"])

	ms.ChangeColl <- ChangeColl{CollName: "t3", SyncId: "5"}

	ms.ChangeColl <- ChangeColl{CollName: "t5", SyncId: "6", BulkWrite: true}
	pending = <-ms.Pending
	require.Equal(t, 2, len(pending))
	require.Equal(t, "2", pending["t2"])
	require.Equal(t, "3", pending["t3"])

	ms.ChangeColl <- ChangeColl{CollName: "t3", SyncId: "5", BulkWrite: true}
	pending = <-ms.Pending
	require.Equal(t, 1, len(pending))
	require.Equal(t, "2", pending["t2"])

	ms.ChangeColl <- ChangeColl{CollName: "t2", SyncId: "2", BulkWrite: true}
	pending = <-ms.Pending
	require.Equal(t, 0, len(pending))
}
