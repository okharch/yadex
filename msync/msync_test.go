package mongosync

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"yadex/config"
)

var c = &config.Config{
	Exchanges: []*config.ExchangeConfig{
		{
			SenderURI:   "mongodb://localhost:27021",
			SenderDB:    "test",
			ReceiverURI: "mongodb://localhost:27023",
			ReceiverDB:  "test",
			RT: map[string]*config.DataSync{"realtime": {
				Delay:   100,
				Batch:   100,
				Exclude: nil,
			},
			},
			ST: map[string]*config.DataSync{".*": {
				Delay:   1000,
				Batch:   500,
				Exclude: []string{"realtime"},
			},
			},
		},
	},
}

func TestNewMongoSync(t *testing.T) {
	// create mongosync
	config.SetLogger()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	var waitExchanges sync.WaitGroup
	ms, err := NewMongoSync(ctx, c.Exchanges[0], &waitExchanges)
	require.Nil(t, err)
	require.NotNil(t, ms)
	// wait for possible oplog processing
}
