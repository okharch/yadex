package mongosync

import (
	"github.com/stretchr/testify/require"
	"testing"
	"yadex/config"
)

func TestGetCollMatch(t *testing.T) {
	c := &config.ExchangeConfig{
		RT: map[string]*config.DataSync{"realtime": {
			Delay:   100,
			Batch:   500,
			Exclude: []string{"not-really"},
		},
		},
		ST: map[string]*config.DataSync{".*": {
			Delay:   100,
			Batch:   500,
			Exclude: []string{"realtime"},
		},
		},
	}
	cm := GetCollMatch(c)
	require.NotNil(t, cm)
	delay, batch, rt := cm("test")
	require.False(t, rt)
	require.Equal(t, 100, delay)
	require.Equal(t, 500, batch)
	// check again it returns the same value
	delay1, batch1, rt1 := cm("test")
	require.Equal(t, delay, delay1)
	require.Equal(t, batch, batch1)
	require.Equal(t, rt, rt1)
	delay, batch, rt = cm("a1-realtime")
	require.True(t, rt)
	require.Equal(t, 100, delay)
	require.Equal(t, 500, batch)
	delay, batch, rt = cm("not-really-realtime")
	require.Equal(t, -1, delay)
}
