package config

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
	"gopkg.in/yaml.v3"
	"os"
)

const (
	STMinDelayDefault = 999
	RTMinDelayDefault = 99
	RTDelayDefault    = 100
	STDelayDefault    = 2000
	RTBatchDefault    = 161000 // if pending Realtime batch reaches this size it will be flushed
	STBatchDefault    = 256000 // if pending ST batch reaches this size it will be flushed
	RTExpiresDefault  = 5000   // 5 seconds this data expires, can drop batch
	DefaultDB         = "IonM"
	DefaultQueue      = 4096
	DefaultCoWrite    = 3
)

type (
	DataSync struct {
		// minimal ms delay before consequential flush of buffers
		MinDelay int `yaml:"MinDelay,omitempty"`
		// max delay before flush
		Delay int `yaml:"Delay,omitempty"`
		// max size before flush
		Batch int `yaml:"Batch,omitempty"`
		// ms before data expires. Expired Realtime Data will not be sent
		Expires int `yaml:"Expires,omitempty"`
		Queue   int `yaml:"Queue,omitempty"`
		// Regexp of colls to exclude
		Exclude []string `yaml:"Exclude,omitempty"`
	}
	ExchangeConfig struct {
		SenderURI   string `yaml:"SenderURI"`
		SenderDB    string `yaml:"SenderDB,omitempty"`
		ReceiverURI string `yaml:"ReceiverURI"`
		ReceiverDB  string `yaml:"ReceiverDB,omitempty"`
		// Queue is the size of batches for BulkWrite
		Queue int `yaml:"Queue,omitempty"`
		// the maximum number of allowed concurrent writes
		CoWrite int                  `yaml:"CoWrite,omitempty"`
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

func FixExchangeConfig(i int, e *ExchangeConfig) error {
	if e.SenderDB == "" {
		e.SenderDB = DefaultDB
	}
	if e.ReceiverDB == "" {
		e.ReceiverDB = DefaultDB
	}
	if e.SenderURI == "" {
		return errors.New("Sender URI is not specified")
	}
	if e.ReceiverURI == "" {
		return errors.New("Receiver URI is not specified")
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
		if r.Batch < 16000 {
			log.Warnf("Default value for Batch buffer is %d is small. It can impede speed of exchange.", r.Batch)
		}
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
		if r.Batch < 16000 {
			log.Warnf("Default value for Batch buffer is %d is small. It can impede speed of exchange.", r.Batch)
		}
	}
	if e.CoWrite <= 0 {
		return fmt.Errorf("Invalid value for CoWrite: %d. Must be greater than zero", e.CoWrite)
	}
	return nil
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
	return result, nil
}
