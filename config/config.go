package config

import (
	"context"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type (
	DataSync struct{
		Delay int // max delay before flush
		Batch int // max size before flush
		Exclude string // Regexp of colls to exclude
	}
	ExchangeConfig       struct {
		SenderURI    string
		SenderDB     string
		ReceiverURI  string
		ReceiverDB   string
		RT map[string] *DataSync
		ST map[string] *DataSync
	}
	Config struct {
		Exchange []	*ExchangeConfig
	}
)
func ReadConfig(configFile string) (*Config, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, errors.Wrapf(err, "was not able to get data from config file %s : %s", configFile, err)
	}
	var result Config
	// replace tab with 4 spaces
	// config = bytes.ReplaceAll(config,[]byte("\t"), []byte("    "))
	err = yaml.Unmarshal(data, &result)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing yaml config %s ", configFile)
	}
	return &result, nil
}

// MakeWatchConfigChannel creates  channel to notify abdout config changes
// normal reaction implies to stop objects that depends on config, recreate them and rerun
// if parent context is done, it closes the channel
// also it listens to os.Interrupt signal. If it occurs it closes the channel
func MakeWatchConfigChannel(ctx context.Context, configFileName string) chan *config.Config {
	configChan := make(chan *Config)
	go func() {
		defer close(configChan)
		osSignal := make(chan os.Signal, 1)
		signal.Notify(osSignal, os.Interrupt)
		signal.Notify(osSignal, syscall.SIGHUP) // reload config
		// watch file configFileName and Ctrl+C signal. Close channel on Ctrl+C
		var oldTime time.Time
		rereadConfig := func() {
			stat, err := os.Stat(configFileName)
			if err != nil {
				log.Errorf("Failed to update stat on %s: %s", configFileName, err)
				return
			}
			oldTime = stat.ModTime()
			log.Infof("reread configuration from %s", configFileName)
			cfg, err := ReadConfig(configFileName)
			if err != nil {
				log.Errorf("Config file %s is invalid: %s", configFileName, err)
				return
			}
			configChan <- cfg
		}
		rereadConfig()
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-osSignal:
				if sig == syscall.SIGHUP {
					rereadConfig()
					continue
				}
				log.Info("Gracefully handling Ctrl+C signal...")
				return
			case <-time.After(time.Second):
				stat, err := os.Stat(configFileName)
				if err != nil {
					log.Errorf("Config file %s not found: %s", configFileName, err)
					continue
				}
				if oldTime == stat.ModTime() {
					continue
				}
				rereadConfig()
			}
		}
	}()
	return configChan
}



