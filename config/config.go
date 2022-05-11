package config

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	STMinDelayDefault = 999
	RTMinDelayDefault = 99
	RTDelayDefault    = 100
	STDelayDefault    = 2000
	RTBatchDefault    = 512  // if pending RT batch reaches this size it will be flushed
	STBatchDefault    = 8192 // if pending ST batch reaches this size it will be flushed
	DefaultDB         = "IonM"
)

type (
	DataSync struct {
		MinDelay int      // minimal ms delay before consequential flush of buffers
		Delay    int      // max delay before flush
		Batch    int      // max size before flush
		Exclude  []string // Regexp of colls to exclude
	}
	ExchangeConfig struct {
		SenderURI   string
		SenderDB    string
		ReceiverURI string
		ReceiverDB  string
		RT          map[string]*DataSync
		ST          map[string]*DataSync
	}
	Config struct {
		Exchanges []*ExchangeConfig
	}
)

func setIntDefault(v *int, d int) {
	if *v == 0 {
		*v = d
	}
}

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
	// now check empty fields to set defaults
	for i, e := range result.Exchanges {
		if e.SenderDB == "" {
			e.SenderDB = DefaultDB
		}
		if e.ReceiverDB == "" {
			e.ReceiverDB = DefaultDB
		}
		// set RT defaults
		for key, r := range e.RT {
			setIntDefault(&r.MinDelay, RTMinDelayDefault)
			setIntDefault(&r.Delay, RTDelayDefault)
			if r.Delay < r.MinDelay {
				log.Warnf("found [%d].RT.%s.Delay(%d) less than MinDelay(%d), set to MinDelay", i, key, r.Delay, r.MinDelay)
				r.Delay = r.MinDelay
			}
			setIntDefault(&r.Batch, RTBatchDefault)
		}
		// set ST defaults
		for key, r := range e.ST {
			setIntDefault(&r.MinDelay, STMinDelayDefault)
			setIntDefault(&r.Delay, STDelayDefault)
			if r.Delay < r.MinDelay {
				log.Warnf("found [%d].ST.%s.Delay(%d) less than MinDelay(%d), set to MinDelay", i, key, r.Delay, r.MinDelay)
				r.Delay = r.MinDelay
			}
			setIntDefault(&r.Batch, STBatchDefault)
		}
	}
	return &result, nil
}

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
				log.Errorf("Watch config file %s error:%s", configFileName, err)
			}
		}
	}()
	return configChan
}

func SetLogger(level log.Level) {
	dir := os.Getenv("TEMP")
	if dir == "" {
		dir = "C:/Temp"
	}
	fileName := dir + "/" + "test.log"
	log.Infof("logging to file %s", fileName)
	//	os.Remove(fileName)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC, 0666)
	go func() {
		for range time.Tick(time.Second) {
			_ = f.Sync()
		}
	}()
	log.SetOutput(f)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	log.SetLevel(level)
	log.SetReportCaller(true)
	Formatter := new(log.TextFormatter)
	//Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z07:00"
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999"
	Formatter.FullTimestamp = true
	//Formatter.ForceColors = true
	log.SetFormatter(Formatter)
}
