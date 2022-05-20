package config

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestMakeWatchConfigChannel(t *testing.T) {
	SetLogger(log.TraceLevel, "")
	c := &Config{
		Exchanges: []*ExchangeConfig{
			{
				SenderURI:   "mongodb://localhost:27021",
				SenderDB:    "IonM",
				ReceiverURI: "mongodb://localhost:27023",
				ReceiverDB:  "IonM",
				RT: map[string]*DataSync{"realtime": {
					MinDelay: 301,
					Delay:    300,
					Expires:  299,
				}},
				ST: map[string]*DataSync{".*": {
					MinDelay: 1100,
					Delay:    1000,
					Batch:    500,
					Exclude:  []string{"realtime"},
				},
				},
			},
		},
	}
	b, err := yaml.Marshal(c)
	f, err := os.CreateTemp("", "TestMakeWatchConfigChannel") // create temp file
	require.NoError(t, err)
	n, err := f.Write(b)
	require.Nil(t, err)
	require.Equal(t, n, len(b))
	require.NoError(t, f.Close())
	ctx, cancel := context.WithCancel(context.TODO())
	cfgChan := MakeWatchConfigChannel(ctx, f.Name())
	require.NotNil(t, cfgChan)
	// first available after watching
	c1 := <-cfgChan
	require.NotEqual(t, c, c1)
	require.Equal(t, len(c.Exchanges), len(c1.Exchanges))
	x := c.Exchanges[0]
	x1 := c1.Exchanges[0]
	require.Equal(t, x.ReceiverURI, x1.ReceiverURI)
	require.Equal(t, x.SenderURI, x1.SenderURI)

	// make sure it set default values for omitted fields in config
	xRT := x.RT["realtime"]
	require.NotNil(t, xRT)
	x1RT := x1.RT["realtime"]
	require.NotNil(t, x1RT)
	require.Equal(t, 301, xRT.MinDelay)
	require.Equal(t, 249, x1RT.MinDelay)
	require.Equal(t, 300, xRT.Delay)
	require.Equal(t, 249, x1RT.Delay)
	require.Equal(t, 0, xRT.Batch)
	require.Equal(t, RTBatchDefault, x1RT.Batch)

	// make sure it fixes error minDelay>MaxDelay
	require.Greater(t, x.ST[".*"].MinDelay, x.ST[".*"].Delay)
	require.GreaterOrEqual(t, x1.ST[".*"].Delay, x1.ST[".*"].MinDelay)

	// make sure it fixes Realtime.Delay for Expires
	require.Greater(t, x.ST[".*"].MinDelay, x.ST[".*"].Delay)
	require.GreaterOrEqual(t, x1.ST[".*"].Delay, x1.ST[".*"].MinDelay)

	// now nothing available for 1 second
	var timeout bool
	waitConfig := func(Delay time.Duration) *Config {
		timeout = false
		select {
		case res := <-cfgChan:
			if res == nil {
				log.Infof("waitConfig: channel is closed, return nil")
			} else {
				log.Infof("waitConfig: obtained new config from cfgChan")
			}
			return res
		case <-time.After(Delay):
			log.Infof("waitConfig: exit on timeout %v", Delay)
			timeout = true
		}
		return nil
	}

	log.Info("file has not been updated, so wait for full 1 second without update")
	c2 := waitConfig(time.Millisecond * 200)
	require.Nil(t, c2)
	// now update config
	x.SenderURI = "s2"
	b, err = yaml.Marshal(c)
	require.NoError(t, err)

	log.Infof("Updating %s file. It will take no more than 1/2 second to detect changes", f.Name())
	err = os.WriteFile(f.Name(), b, 0666)
	require.NoError(t, err)
	c2 = waitConfig(time.Millisecond * 30)
	require.NotNil(t, c2)
	x2 := c2.Exchanges[0]
	require.Equal(t, x.SenderURI, x2.SenderURI)

	log.Infof("Keep %s file untouched. It will not return new config", f.Name())
	c3 := waitConfig(time.Millisecond * 200)
	require.True(t, timeout)
	require.Nil(t, c3)

	log.Infof("Now use SIGHUP to force reading config from %s", f.Name())
	osSignal <- syscall.SIGHUP
	c3 = waitConfig(time.Millisecond * 10)
	require.NotNil(t, c3)
	x3 := c3.Exchanges[0]
	require.Equal(t, x.SenderURI, x3.SenderURI)

	// now test SIGTERM
	osSignal <- os.Interrupt
	// channel should be closed, c would be nil
	c = waitConfig(time.Millisecond)
	require.False(t, timeout)
	require.Nil(t, c)

	// now test closing channel on expired context
	cfgChan = MakeWatchConfigChannel(ctx, f.Name())
	require.NotNil(t, cfgChan)
	c = waitConfig(time.Millisecond)
	require.False(t, timeout)
	require.NotNil(t, c)
	cancel()
	// channel should be closed, c would be nil
	c = waitConfig(time.Millisecond)
	require.False(t, timeout)
	require.Nil(t, c)

	// finally remove temporary file
	require.NoError(t, os.Remove(f.Name())) // remove temp file
}
