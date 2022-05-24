package config

import (
	"context"
	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// MakeWatchConfigChannel creates  channel to notify about config changes
// normal reaction implies to stop objects that depends on config, recreate them and rerun
// if parent context is done, it closes the channel
// also it listens to os.Interrupt signal. If it occurs it closes the channel
var osSignal chan os.Signal

func MakeWatchConfigChannel(ctx context.Context, configFileName string) chan *Config {
	configChan := make(chan *Config)
	go func() {
		defer close(configChan)
		osSignal = make(chan os.Signal, 1)
		signal.Notify(osSignal, os.Interrupt)
		signal.Notify(osSignal, syscall.SIGHUP) // reload config
		// watch file configFileName and Ctrl+C signal. Close channel on Ctrl+C
		rereadConfig := func() {
			log.Infof("reread configuration from %s", configFileName)
			cfg, err := ReadConfig(configFileName)
			if err != nil {
				log.Errorf("failed to read config file %s: %s", configFileName, err)
				return
			}
			configChan <- cfg
		}
		rereadConfig()
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Errorf("failed to establish file watcher:%s", err)
			return
		}
		defer func() {
			_ = watcher.Close()
		}()
		err = watcher.Add(configFileName)
		if err != nil {
			log.Errorf("failed to create watcher on file %s", configFileName)
			return
		}
		const infiniteDuration = time.Hour * 10000
		postponeReload := infiniteDuration
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-osSignal:
				if sig == syscall.SIGHUP {
					log.Info("Rereading config on SIGHUP signal...")
					rereadConfig()
					continue
				}
				log.Info("Gracefully handling Ctrl+C signal...")
				return
			case event := <-watcher.Events:
				log.Debugf("Watch config event:%+v", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					// postpone reload as usually there fre Write events and we want to reload only once
					postponeReload = time.Millisecond * 5
				}
			case <-time.After(postponeReload):
				postponeReload = infiniteDuration
				rereadConfig()
			case err := <-watcher.Errors:
				log.Errorf("Watch config file %s error: %s", configFileName, err)
			}
		}
	}()
	return configChan
}
