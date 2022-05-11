package mongosync

import (
	"github.com/stretchr/testify/require"
	"testing"
	"yadex/config"
)

func TestGetCollMatch(t *testing.T) {
	c := &config.ExchangeConfig{
		RT: map[string]*config.DataSync{"realtime": {
			Delay:   99,
			Batch:   512,
			Exclude: []string{"not-really"},
		},
		},
		ST: map[string]*config.DataSync{".*": {
			Delay:   500,
			Batch:   8192,
			Exclude: []string{"realtime"},
		},
		},
	}
	cm := GetCollMatch(c)
	require.NotNil(t, cm)
	cfg, rt := cm("test")
	require.NotNil(t, cfg)
	require.False(t, rt)
	require.Equal(t, config.STMinDelayDefault, cfg.MinDelay)
	require.Equal(t, 500, cfg.Delay)
	require.Equal(t, 8192, cfg.Batch)
	// check again it returns the same value
	cfg1, rt1 := cm("test")
	require.NotNil(t, cfg1)
	require.Equal(t, cfg, cfg1)
	require.Equal(t, rt, rt1)
	cfg, rt = cm("a1-realtime")
	require.NotNil(t, cfg)
	require.True(t, rt)
	require.Equal(t, 99, cfg.Delay)
	require.Equal(t, 512, cfg.Batch)
	cfg, rt = cm("not-really-realtime")
	require.Nil(t, cfg)
}
