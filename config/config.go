package config

import (
	"context"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
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
	RTBatchDefault    = 512  // if pending Realtime batch reaches this size it will be flushed
	STBatchDefault    = 8192 // if pending ST batch reaches this size it will be flushed
	RTExpiresDefault  = 5000 // 5 seconds this data expires, can drop batch
	DefaultDB         = "IonM"
	DefaultQueue      = 4096
	DefaultCoWrite    = 3
)

type (
	DataSync struct {
		// minimal ms delay before consequential flush of buffers
		MinDelay int `yaml:"MinDelay"`
		// max delay before flush
		Delay int `yaml:"Delay"`
		// max size before flush
		Batch int `yaml:"Batch"`
		// ms before data expires. Expired Realtime Data will not be sent
		Expires int `yaml:"Expires"`
		// Queue is the size of batches each collection handler can keep for BulkWrite
		Queue int `yaml:"Queue"`
		// Regexp of colls to exclude
		Exclude []string `yaml:"Exclude"`
	}
	ExchangeConfig struct {
		SenderURI   string `yaml:"SenderURI"`
		SenderDB    string `yaml:"SenderDB"`
		ReceiverURI string `yaml:"ReceiverURI"`
		ReceiverDB  string `yaml:"ReceiverDB"`
		// Queue is the global default for size of queue that collection channel can keep for each collection.
		// big values can affect memory consumption but they deliver better following oplog tail
		// You can also specify this size for individual collections on ST/Realtime records
		Queue int `yaml:"Queue"`
		// the maximum number of allowed concurrent writes
		CoWrite int                  `yaml:"CoWrite"`
		RT      map[string]*DataSync `yaml:"Realtime"`
		ST      map[string]*DataSync `yaml:"ST"`
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

func Max[T constraints.Ordered](s ...T) T {
	if len(s) == 0 {
		var zero T
		return zero
	}
	m := s[0]
	for _, v := range s {
		if m < v {
			m = v
		}
	}
	return m
}

func FixExchangeConfig(i int, e *ExchangeConfig) {
	if e.SenderDB == "" {
		e.SenderDB = DefaultDB
	}
	if e.ReceiverDB == "" {
		e.ReceiverDB = DefaultDB
	}
	setIntDefault(&e.CoWrite, DefaultCoWrite)
	e.CoWrite = Max(e.CoWrite, 2)
	setIntDefault(&e.Queue, DefaultQueue)
	//workers := runtime.NumCPU()
	// set Realtime defaults
	for key, r := range e.RT {
		// set Delay defaults for Realtime
		setIntDefault(&r.MinDelay, RTMinDelayDefault)
		setIntDefault(&r.Delay, RTDelayDefault)
		setIntDefault(&r.Expires, RTExpiresDefault)
		setIntDefault(&r.Queue, e.Queue)
		// fix Delays according to Expires < MinDelay < Delay
		if r.MinDelay >= r.Expires {
			log.Warnf("found [%d].Realtime.%s.MinDelay(%d) >= than Expires(%d), set to Expires-50", i, key, r.MinDelay, r.Expires)
			r.MinDelay = r.Expires - 50
		}
		if r.Delay >= r.Expires {
			log.Warnf("found [%d].Realtime.%s.Delay(%d) >= than Expires(%d), set to Expires-50", i, key, r.Delay, r.Expires)
			r.Delay = r.Expires - 50
		}
		if r.Delay < r.MinDelay {
			log.Warnf("found [%d].Realtime.%s.Delay(%d) less than MinDelay(%d), set to MinDelay", i, key, r.Delay, r.MinDelay)
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

func FixConfig(cfg *Config) {
	// now check empty fields to set defaults
	for i, e := range cfg.Exchanges {
		FixExchangeConfig(i, e)
	}
}

func ReadConfig(configFile string) (*Config, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, errors.Wrapf(err, "was not able to get data from config file %s : %s", configFile, err)
	}
	result := &Config{}
	// replace tab with 4 spaces
	// config = bytes.ReplaceAll(config,[]byte("\t"), []byte("    "))
	err = yaml.Unmarshal(data, result)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing yaml config %s ", configFile)
	}
	FixConfig(result)
	return result, nil
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
				log.Errorf("Watch config file %s error: %s", configFileName, err)
			}
		}
	}()
	return configChan
}

func SetLogger(level log.Level, logFile string) {
	log.SetLevel(level)
	log.SetReportCaller(true)
	Formatter := new(log.TextFormatter)
	//Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z07:00"
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999"
	Formatter.FullTimestamp = true
	//Formatter.ForceColors = true
	log.SetFormatter(Formatter)
	// logFile is more error prone, setup it last
	if logFile == "" {
		logFile = os.Getenv("YADEX_LOG")
	}
	if logFile != "" {
		log.Infof("logging to file %s", logFile)
		//	os.Remove(fileName)
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Errorf("error opening file %s: %v", logFile, err)
			return
		}
		//go func() {
		//	for range time.Tick(time.Second) {
		//		_ = f.Sync()
		//	}
		//}()
		log.SetOutput(f)
	}
}
