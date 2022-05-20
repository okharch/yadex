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
	cm := []byte("test")
	stMatch := compileRegexps(c.ST)
	require.NotNil(t, stMatch)
	cfg := findEntry(stMatch, cm)
	require.NotNil(t, cfg)
	require.Equal(t, 500, cfg.Delay)
	require.Equal(t, 8192, cfg.Batch)
	rtMatch := compileRegexps(c.RT)
	cm = []byte("a1-realtime")
	cfg = findEntry(stMatch, cm)
	require.Nil(t, cfg)
	cfg = findEntry(rtMatch, cm)
	require.NotNil(t, cfg)
	require.Equal(t, 99, cfg.Delay)
	require.Equal(t, 512, cfg.Batch)
	cm = []byte("not-really-realtime")
	cfg = findEntry(rtMatch, cm)
	require.Nil(t, cfg)
}
