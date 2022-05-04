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
	SetLogger()
	c := &Config{
		Exchanges: []*ExchangeConfig{
			{
				SenderURI:   "mongodb://localhost:27021",
				SenderDB:    "IonM",
				ReceiverURI: "mongodb://localhost:27023",
				ReceiverDB:  "IonM",
				RT: map[string]*DataSync{"realtime": {
					Delay:   100,
					Batch:   100,
					Exclude: nil,
				},
				},
				ST: map[string]*DataSync{".*": {
					Delay:   1000,
					Batch:   500,
					Exclude: []string{"realtime"},
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
	ctx := context.TODO()
	cfgChan := MakeWatchConfigChannel(ctx, f.Name())
	require.NotNil(t, cfgChan)
	// first available after watching
	c1 := <-cfgChan
	require.NotEqual(t, c, c1)
	require.Equal(t, 1, len(c1.Exchanges))
	x := c.Exchanges[0]
	x1 := c1.Exchanges[0]
	require.Equal(t, x.ReceiverURI, x1.ReceiverURI)
	require.Equal(t, x.SenderURI, x1.SenderURI)
	// now nothing available for 1 second
	waitConfig := func(Delay time.Duration) *Config {
		select {
		case res := <-cfgChan:
			return res
		case <-time.After(Delay):
		}
		return nil
	}
	log.Info("file has not been updated, so wait for full 3 second without update")
	c2 := waitConfig(time.Second * 3)
	require.Nil(t, c2)
	// now update config
	x.SenderURI = "s2"
	b, err = yaml.Marshal(c)
	require.NoError(t, err)
	log.Infof("Updating %s file. It will take no more than two second to detect changes", f.Name())
	err = os.WriteFile(f.Name(), b, 0666)
	require.NoError(t, err)
	c2 = waitConfig(time.Second * 10)
	x2 := c2.Exchanges[0]
	require.NotNil(t, c2)
	require.Equal(t, x.SenderURI, x2.SenderURI)
	// now update config and send syscall.SIGHUP to osSignal
	x.SenderURI = "s3"
	b, err = yaml.Marshal(c)
	require.NoError(t, err)
	log.Infof("Updating %s file. But wait only 10ms so it would not be able to detect changes", f.Name())
	err = os.WriteFile(f.Name(), b, 0666)
	require.NoError(t, err)
	c3 := waitConfig(time.Millisecond * 10)
	require.Nil(t, c3)
	// send SIGHUP asynchronously
	log.Info("Sending SIGHUP signal to help detect changes immediately")
	go func() {
		osSignal <- syscall.SIGHUP
	}()
	c3 = waitConfig(time.Millisecond * 10)
	require.NotNil(t, c3)
	x3 := c3.Exchanges[0]
	require.Equal(t, x.SenderURI, x3.SenderURI)
	require.NoError(t, os.Remove(f.Name())) // remove temp file
}
func SetLogger() {
	Formatter := new(log.TextFormatter)
	//Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z07:00"
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999"
	Formatter.FullTimestamp = true
	Formatter.ForceColors = true
	log.SetFormatter(Formatter)
	log.SetLevel(log.DebugLevel)
}
